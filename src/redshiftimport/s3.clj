(ns
  ^{:doc
    "
     Usage:
      (def client (s3/connect! {:access-key \"bla\" :secret-key \"bla\" :region \"eu-central-1\"}))
      then use list or stream->s3! to list or load files
    "}
  redshiftimport.s3
  (:import [java.io InputStream ByteArrayInputStream]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model PutObjectRequest ObjectMetadata PutObjectResult]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.regions Region RegionUtils]
           [com.amazonaws.event ProgressListener]
           [org.apache.commons.lang StringUtils]))


(defrecord Ctx [^AmazonS3Client client])
(defrecord S3File [bucket file])

(defn ^PutObjectRequest put-req [^String bucket ^String file ^InputStream in content-len]
  (PutObjectRequest. bucket file in (doto (ObjectMetadata.)
                                      (.setContentLength (int content-len)))))

(defn as-s3-fqn
  "Ensures that the filename starts with s3://"
  [^String file]
  (if (.startsWith file "s3://")
    file
    (apply str "s3://" (drop-while #(= \/ %) file))))

(defn test-input [] (ByteArrayInputStream. (.getBytes "TESTFILE" "UTF-8")))

(defn remote-all-slashes [^String s]
  (StringUtils/replace s "/" ""))

(defn stream->s3!
  "This operation copy a input stream to a s3 bucket,
   throws an exception if any error
   calls close in the InputStream after its used"
  [{:keys [^AmazonS3Client client]} ^InputStream in content-len {:keys [bucket file]}]
  {:pre [client in (integer? content-len) (string? bucket) (string? file)]}
  (try
    (.putObject client (put-req (remote-all-slashes bucket) file in content-len))
    (finally
      (.close in))))

(defn delete-file! [{:keys [^AmazonS3Client client]} bucket file]
  {:pre [client bucket file]}
  (.deleteObject client (str bucket) (str file)))

(defn connect! [{:keys [access-key secret-key region]}]
  {:pre [(string? access-key) (string? secret-key) (string? region)]}
  (let [creds    (BasicAWSCredentials. (str access-key) (str secret-key))
        _        (do (prn (map str (RegionUtils/getRegions))))
        region   (RegionUtils/getRegion (str region))

        s3client (doto
                   (AmazonS3Client. ^BasicAWSCredentials creds)
                   (.setRegion ^Region region))]
    (->Ctx s3client)))
