(ns
  ^{:doc
    "
     Usage:
      (def client (s3/connect! {:access-key \"bla\" :secret-key \"bla\" :region \"eu-central-1\"}))
      then use list or stream->s3! to list or load files
    "}
  redshiftimport.s3
  (:import [java.io InputStream]
           [com.amazonaws.services.s3 AmazonS3Client]
           [com.amazonaws.services.s3.model PutObjectRequest ObjectMetadata PutObjectResult]
           [com.amazonaws.auth BasicAWSCredentials]
           [com.amazonaws.regions Region RegionUtils]))


(defrecord Ctx [^AmazonS3Client client])
(defrecord S3File [bucket file])

(defn ^PutObjectRequest put-req [^String bucket ^String file ^InputStream in content-len]
  (PutObjectRequest. bucket file in (doto (ObjectMetadata.)
                                      (.setContentLength (int content-len)))))


(defn list [{:keys [^AmazonS3Client client]} bucket k]
  (map #(.getKey %) (-> client
                        (.listObjects (str bucket) (str k))
                        .getObjectSummaries)))

(defn stream->s3!
  "This operation copy a input stream to a s3 bucket,
   throws an exception if any error"
  [{:keys [^AmazonS3Client client]} ^InputStream in content-len {:keys [bucket file]}]
  {:pre [client in (integer? content-len) (string? bucket) (string? file)]}
  (.putObject client (put-req bucket file in content-len)))


(defn connect! [{:keys [access-key secret-key region]}]
  {:pre [(string? access-key) (string? secret-key) (string? region)]}
  (let [creds    (BasicAWSCredentials. (str access-key) (str secret-key))
        _        (do (prn (map str (RegionUtils/getRegions))))
        region   (RegionUtils/getRegion (str region))

        s3client (doto
                   (AmazonS3Client. ^BasicAWSCredentials creds)
                   (.setRegion ^Region region))]
    (->Ctx s3client)))
