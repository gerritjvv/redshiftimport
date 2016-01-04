(ns redshiftimport.core
  (:require [redshiftimport.hdfs :as hdfs]
            [redshiftimport.s3 :as s3]
            [redshiftimport.redshift :as redshift]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class)
  (:import [java.util.concurrent Executors ExecutorService Future]
           [com.amazonaws.regions RegionUtils]
           [com.amazonaws.util StringInputStream]
           [java.io InputStream]
           [org.apache.commons.lang StringUtils]
           [clojure.lang ArityException]))


(defn remove-double-slashes [s]
  (StringUtils/replace (str s) "//" "/"))

(defn remove-s3-prefix [s]
  (StringUtils/replace (str s) "s3:" ""))

(defn remove-trailing-slash [s]
  (if (.endsWith (str s) "/")
    (.substring (str s) 0 (dec (count s)))
    s))

(defn remove-starting-slash [s]
  (if (.startsWith (str s) "/")
    (.substring (str s) 1)
    s))


(def sanitise-s3-path (comp remove-s3-prefix remove-double-slashes))

(defn choose-file-name
  "Return either a file name that depends in the hdfs directories and the hdfs file name
   or a unique file name created from start-ts and i"
  [hdfs-file hdfs-s3-prefix-depth start-ts i]
  (if hdfs-s3-prefix-depth
    (str (hdfs/choose-hdfs-prefix-dir hdfs-file hdfs-s3-prefix-depth) "_" (hdfs/file-name hdfs-file))
    (str (hdfs/file-name hdfs-file) "_" start-ts "_" i)))

(defn hdfs-file->s3
  "hdfs-ctx: the hdfs context
   s3-ctx : s3 context
   s3bucket: complete s3 bucket, must start with /
   s3path: the s3 key
   i: unique index that will be added to the file name, this is used incase multiple dirs are globbed and files have the same name

   return the s3bucket/filenameuploaded"
  [hdfs-ctx hdfs-file s3-ctx s3bucket s3path hdfs-s3-prefix-depth start-ts i]
  (let [file-name (str s3path "/" (choose-file-name hdfs-file hdfs-s3-prefix-depth start-ts i))
        content-len (hdfs/content-length hdfs-ctx hdfs-file)
        input (hdfs/input-stream hdfs-ctx hdfs-file)]
    (prn "load to s3 file " (sanitise-s3-path (str s3bucket "/" file-name)) content-len)

    (try
      (prn "calling s3/stream")
      (s3/wait-on-upload! (s3/stream->s3! s3-ctx input content-len {:bucket (sanitise-s3-path (remove-trailing-slash s3bucket)) :file (sanitise-s3-path (remove-starting-slash file-name))}))

      (finally
        (.close ^InputStream input)))
    (s3/as-s3-fqn (sanitise-s3-path (str s3bucket "/" file-name)))))

(defn pmap2
  "Run the coll in its own ExecutorService and return the result of (map f coll), f is called with (f start-ts-nanos index coll-item)"
  [threads f coll]
  (let [^ExecutorService exec (Executors/newFixedThreadPool (int threads))
        ^Callable submit-f (fn [start-ts index item] #(f start-ts index item))
        start-ts (System/nanoTime)
        counter-a (atom 0)]

    (try
      (map
        deref
        (transduce (comp
                     (map #(.submit exec (submit-f start-ts (swap! counter-a inc) %)))
                     (map #(delay (do
                                    (prn "Waiting on future " %)
                                    (.get ^Future %)))))
                   conj
                   coll))
      (finally
        (.shutdown exec)))))

(defn sleep [ms]
  (Thread/sleep (long ms)))

(defn retry
  "Returns a function than when called with n will call f with args once and retry n times  in case of failure
   f = (rery my-f arg arg arg)
   (f 3 1000) == retry 3 times sleeping 1000ms between each retry
   Note: ArityExceptions will not retry"
  [f & args]
  (fn inner [n sleep-ms]
    (try
      (apply f args)
      (catch ArityException ar (throw ar))
      (catch Exception e (if (pos? n)
                           (do
                             (prn "Retrying task sleeping for " sleep-ms "ms")
                             (sleep sleep-ms)
                             (inner (dec n) sleep-ms))
                           (throw e))))))


(defn hdfs->s3-retry [hdfs-ctx s3-ctx s3bucket s3path hdfs-s3-prefix-depth start-ts i hdfs-file]
  (let [retry-f (retry hdfs-file->s3 hdfs-ctx hdfs-file s3-ctx s3bucket s3path hdfs-s3-prefix-depth start-ts i)]
    (retry-f 5 5000)))

(defn hdfs->s3
  "Copy all the hdfs files identified by the glob to the s3path"
  [hdfs-ctx s3-ctx threads hdfs-dir s3bucket s3path hdfs-s3-prefix-depth]
  (let [hdfs-files (hdfs/list-paths hdfs-ctx hdfs-dir)]
    (try
      (doall (pmap2 threads (partial hdfs->s3-retry
                                     hdfs-ctx
                                     s3-ctx
                                     s3bucket
                                     s3path
                                     hdfs-s3-prefix-depth) hdfs-files))
      (finally
        (prn "Done copying to s3")))))

(defn create-manifest [s3-files]
  (redshift/manifest-file s3-files))

(defn load-manifest [red-ctx s3-ctx redshift-table manifest s3-path s3-bucket s3-access s3-secret format]
  (let [^InputStream manifest-input (StringInputStream. ^String manifest)
        manifest-filename (str s3-path "/manifest_" (System/nanoTime))
        manifest-fqn (s3/as-s3-fqn (str s3-bucket "/"  manifest-filename))]
    (prn "uploading manifest")
    (s3/wait-on-upload! (s3/stream->s3! s3-ctx manifest-input (.available manifest-input) {:bucket s3-bucket :file manifest-filename}))
    (redshift/upload-as-manifest red-ctx redshift-table manifest-fqn s3-access s3-secret format)
    (prn "done loading manifest")))

(defn exec [{:keys [redshift-url
                    redshift-user
                    redshift-pwd
                    redshift-table
                    s3-access
                    s3-secret
                    s3-region
                    s3-bucket
                    s3-path
                    hdfs-url
                    hdfs-path
                    hdfs-s3-prefix-depth
                    threads
                    delete-s3
                    disable-redshift
                    manifest-size
                    format]}]
  (let [manifest-size (if manifest-size manifest-size 20)
        s3-ctx (s3/connect! {:access-key s3-access :secret-key s3-secret :region s3-region})
        hdfs-ctx (hdfs/connect! {:default-fs hdfs-url})
        red-ctx (when (not disable-redshift) (redshift/connect! redshift-url redshift-user redshift-pwd))
        s3-files (hdfs->s3 hdfs-ctx s3-ctx threads hdfs-path s3-bucket s3-path hdfs-s3-prefix-depth)
        manifests (map create-manifest (partition-all manifest-size s3-files))]

    (prn "Completed upload of " (count s3-files) " files to s3")
    (when (not disable-redshift)
      (doseq [manifest manifests]
        (load-manifest red-ctx s3-ctx redshift-table manifest s3-path s3-bucket s3-access s3-secret format)))

    (when delete-s3
      (doseq [s3-file s3-files]
        (s3/delete-file! s3-ctx s3-bucket s3-file)))

    (s3/close s3-ctx)
    (prn "done")))

;;;;;;;;;;;;;;;;;;;
;;;;;;;;;CLI

(def cli-options
  [["-r" "--redshift-url jdbc-redshift-url" "JDBC Redshift URL"]
   ["-u" "--redshift-user redshift-user" "JDBC Redshift User"]
   ["-p" "--redshift-pwd redshift-pwd" "JDBC Redshift Password"]
   ["-manifest-size" "--manifest-size number" "Number of files grouped in each manifest, this is needed for huge loads to avoid timeouts between the local server calling and redshift"
    :default 20
    :parse-fn #(Integer/parseInt %)]
   ["-t" "--redshift-table redshift-table" "Redshift table"]
   ["-a" "--s3-access s3-access-key" "S3 access key"]
   ["-s" "--s3-secret s3-secret-key" "S3 secret key"]
   ["-x" "--s3-region s3-region" "S3 region see http://docs.aws.amazon.com/general/latest/gr/rande.html"
    :validate [#(RegionUtils/getRegion %) (str "Must be one of " (mapv str (RegionUtils/getRegions)))]]

   ["-b" "--s3-bucket s3-bucket" "S3 bucket key"]
   ["-z" "--s3-path s3-path" "S3 path key"]
   ["-y" "--hdfs-url fs-default" "Default hdfs name e.g hdfs://mynamenode"]
   ["-d" "--hdfs-path hdfs-path" "Should be a glob e.g /tmp/files/*"]
   ["-q" "--threads threads" "Number of threads to use for uploads to s3"
    :default 4
    :parse-fn #(Integer/parseInt %)]

   ["-delete-s3" "--delete-s3" "if specified the s3 uploads are deleted after uploading"]

   ["-disable-redshift" "--disable-redshift" "if specified the the files are not uploaded to redshift"]
   ["-hdfs-s3-prefix-depth" "--hdfs-s3-prefix-depth number" "if specified the file name is <number levels of hdfs dir compressed e.g /a/b/c/file can be at number 2 b_c>_<hdfs_file_name>"
    :parse-fn #(when % (Integer/parseInt %))]

   ["-format" "--format format" "AVRO, CSV, GZIP (gzip is gzip + csv)"
    :default "csv"]

   ["-h" "--help"]])

(defn prn-help [data]
  (prn data))

(defn -main [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (prn "option " options)
    (cond
      errors (do (prn-help errors) (System/exit (int -1)))
      (:help options) (prn-help summary)
      :default (try
                 (exec options)
                 (catch Exception e (do
                                      (.printStackTrace e)
                                      (prn "Error  " e)
                                      (System/exit (int -1))))))))
