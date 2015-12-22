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
           [org.apache.commons.lang StringUtils]
           [java.util.concurrent ExecutorService Executors]
           [com.amazonaws.services.s3.transfer TransferManager Upload]))


(defrecord Ctx [^AmazonS3Client client ^TransferManager transfer-manager])
(defrecord S3File [bucket file])

(defonce TEN-MB 10485760)

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

(defn
  ^Upload
  stream->s3!
  "This operation copy a input stream to a s3 bucket,
   throws an exception if any error
   does not close the InputStream"
  [{:keys [^TransferManager transfer-manager]} ^InputStream in content-len {:keys [bucket file]}]
  {:pre [transfer-manager in (integer? content-len) (string? bucket) (string? file)]}
  ;; request.getRequestClientOptions.setReadLimit(TEN_MB)
  ;;uploader.getConfiguration.setMultipartUploadThreshold(TEN_MB)
  (let [^PutObjectRequest putreq (put-req bucket file in content-len)]
    ;; can't use in hadoop-aws yet this method doesn't exist
    ;(.setReadLimit (.getRequestClientOptions putreq) (int (* TEN-MB 5)))
    (.upload transfer-manager putreq)))

(defn wait-on-upload!
  "Wait on the upload object returned from stream->s3!"
  [^Upload upload]
  (.waitForUploadResult upload))

(defn delete-file! [{:keys [^AmazonS3Client client]} bucket file]
  {:pre [client bucket file]}
  (.deleteObject client (str bucket) (str file)))

(defn connect! [{:keys [access-key secret-key region exec]}]
  {:pre [(string? access-key) (string? secret-key) (string? region)]}
  (let [exec1 (if exec exec (Executors/newFixedThreadPool 4))
        creds    (BasicAWSCredentials. (str access-key) (str secret-key))
        _        (do (prn (map str (RegionUtils/getRegions))))
        region   (RegionUtils/getRegion (str region))

        s3client (doto
                   (AmazonS3Client. ^BasicAWSCredentials creds)
                   (.setRegion ^Region region))
        ^TransferManager transfer-manager (TransferManager. s3client exec1)]
    (.setMultipartUploadThreshold (.getConfiguration transfer-manager) (int (* TEN-MB 40)))
    (->Ctx s3client transfer-manager)))


