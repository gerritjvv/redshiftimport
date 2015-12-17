(ns
  ^{:doc "Exmaple jdbc:redshift://examplecluster.abc123xyz789.us-west-2.redshift.amazonaws.com:5439/dev"}
  redshiftimport.redshift
  (:require [sjdbc.core :as sjdbc]))


(defn exec! [conn sql]
  (sjdbc/exec conn sql))

(defn connect! [jdbc-url user pwd]
  (sjdbc/open jdbc-url user pwd {}))