(ns
  ^{:doc "Exmaple jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev"}
  redshiftimport.redshift
  (:require [sjdbc.core :as sjdbc]
            [clojure.data.json :as json]))


(defn exec! [conn sql]
  (sjdbc/exec conn sql))

(defn manifest-file [files]
  (json/write-str
    {"entries" (mapv (fn [file]
                       {"url" (str file) "mandatory" true}) files)}))

(defn upload-as-manifest [conn redshift-table manifest-path s3-access s3-secret]
  (let [sql (str "copy " redshift-table " from  '" manifest-path "'"
                 " credentials"
                 " 'aws_access_key_id=" s3-access
                 ";aws_secret_access_key=" s3-secret
                 "' FORMAT AS AVRO 'auto' manifest")]
    (prn "running redshift sql " sql)
    (exec! conn
           sql)))

(defn connect! [jdbc-url user pwd]
  (sjdbc/open jdbc-url user pwd {}))