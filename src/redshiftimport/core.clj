(ns redshiftimport.core
  (:require [redshiftimport.hdfs :as hdfs]
            [redshiftimport.s3 :as s3]
            [redshiftimport.redshift :as redshift]
            [clojure.tools.cli :refer [parse-opts]])
  (:gen-class)
  (:import [java.util.concurrent Executors ExecutorService Future]
           [java.util.concurrent.atomic AtomicBoolean]
           [com.amazonaws.regions RegionUtils]))


(defn hdfs-file->s3
  "hdfs-ctx: the hdfs context
   s3-ctx : s3 context
   s3bucket: complete s3 bucket, must start with /
   s3path: the s3 key
   i: unique index that will be added to the file name, this is used incase multiple dirs are globbed and files have the same name

   return the s3bucket/filenameuploaded"
  [hdfs-ctx hdfs-file s3-ctx s3bucket s3path i]
  (let [file-name (str s3path "/" (hdfs/file-name hdfs-file) "_" i)
        input (hdfs/input-stream hdfs-ctx hdfs-file)
        content-len (hdfs/content-length hdfs-ctx hdfs-file)]
    (prn "put file " file-name content-len)

    (s3/stream->s3! s3-ctx input content-len {:bucket s3bucket :file file-name})

    (str s3bucket "/" file-name)))

(defn pmap2
  "Run the coll in its own ExecutorService and return the result of (map f coll)"
  [threads f coll]
  (let [^ExecutorService exec (Executors/newFixedThreadPool (int threads))
        ^Callable submit-f (fn [index item] #(f index item))
        counter-a (atom 0)]

    (try
      (map
        deref
        (transduce (comp
                     (map #(.submit exec (submit-f (swap! counter-a inc) %)))
                     (map #(delay (.get ^Future %))))
                   conj
                   coll))
      (finally
        (.shutdown exec)))))

(defn hdfs->s3
  "Copy all the hdfs files identified by the glob to the s3path"
  [hdfs-ctx s3-ctx hdfs-dir s3bucket s3path]
  (prn "HDFS DIR " hdfs-dir)
  (prn "LIST Files " )
  (let [hdfs-files (hdfs/list-paths hdfs-ctx hdfs-dir)]
    (pmap2 4 #(hdfs-file->s3 hdfs-ctx %2 s3-ctx s3bucket s3path %1) hdfs-files)))

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
                    hdfs-path]}]
  (let []
    (let [s3-ctx (s3/connect! {:access-key s3-access :secret-key s3-secret :region s3-region})
          hdfs-ctx (hdfs/connect! {:default-fs hdfs-url})
          red-ctx (redshift/connect! redshift-url redshift-user redshift-pwd)
          s3-files (hdfs->s3 hdfs-ctx s3-ctx hdfs-path s3-bucket s3-path)]

      s3-files)))

;;;;;;;;;;;;;;;;;;;
;;;;;;;;;CLI

(def cli-options
  [["-r" "--redshift-url jdbc-redshift-url" "JDBC Redshift URL"]
   ["-u" "--redshift-user redshift-user" "JDBC Redshift User"]
   ["-p" "--redshift-pwd redshift-pwd" "JDBC Redshift Password"]
   ["-t" "--redshift-table redshift-table" "Redshift table"]
   ["-a" "--s3-access s3-access-key" "S3 access key"]
   ["-s" "--s3-secret s3-secret-key" "S3 secret key"]
   ["-x" "--s3-region s3-region" "S3 region see http://docs.aws.amazon.com/general/latest/gr/rande.html"
    :validate [#(RegionUtils/getRegion %) (str "Must be one of " (mapv str (RegionUtils/getRegions)))]]
   ["-b" "--s3-bucket s3-bucket" "S3 bucket key"]
   ["-z" "--s3-path s3-path" "S3 path key"]
   ["-y" "--hdfs-url fs-default" "Default hdfs name e.g hdfs://mynamenode"]
   ["-d" "--hdfs-path hdfs-path" "Should be a glob e.g /tmp/files/*"]
   ["-h" "--help"]])

(defn prn-help [data]
  (prn data))

(defn -main [& args]
  (let [{:keys [options errors summary]} (parse-opts args cli-options)]
    (prn "option " options)
    (cond
      errors (do (prn-help errors) (System/exit (int -1)))
      (:help options) (prn-help summary)
      :default (exec options))))
