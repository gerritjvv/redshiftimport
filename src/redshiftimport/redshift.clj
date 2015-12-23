(ns
  ^{:doc "Exmaple jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev"}
  redshiftimport.redshift
  (:require [sjdbc.core :as sjdbc]
            [clojure.data.json :as json]
            [clojure.string :as string]))


(defn exec! [conn sql]
  (sjdbc/exec conn sql))

(defn manifest-file [files]
  (json/write-str
    {"entries" (mapv (fn [file]
                       {"url" (str file) "mandatory" true}) files)}))

(defonce default-csv-options ["emptyasnull"
                              "blanksasnull"
                              "maxerror 5"
                              "delimiter ','"
                              "timeformat 'YYYY-MM-DD HH:MI:SS'"
                              "FORMAT AS CSV"
                              "QUOTE AS '\"'"
                              "manifest"])

(defmulti copy-format-options (fn [s] (string/lower-case (name s))))

(defmethod copy-format-options "gzip" [_]
  (string/join " " (conj default-csv-options
                         "gzip")))

(defmethod copy-format-options "avro" [_]
  (string/join " " (string/join (conj default-csv-options
                                      "FORMAT AS AVRO 'auto'"))))

(defmethod copy-format-options :default [_]
  (string/join " " default-csv-options))


(defn upload-as-manifest [conn redshift-table manifest-path s3-access s3-secret format]
  (let [sql (str "copy " redshift-table " from  '" manifest-path "'"
                 " credentials"
                 " 'aws_access_key_id=" s3-access
                 ";aws_secret_access_key=" s3-secret
                 "' "
                 (copy-format-options format))]
    (prn "running redshift sql " sql)
    (exec! conn
           sql)))

(defn connect! [jdbc-url user pwd]
  (sjdbc/open jdbc-url user pwd {}))