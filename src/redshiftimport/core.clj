(ns redshiftimport.core
  (:require [redshiftimport.hdfs :as hdfs]
            [redshiftimport.s3 :as s3]
            [redshiftimport.redshift :as redshift])
  (:gen-class))


(defn hdfs-file->s3 [hdfs-ctx hdfs-file s3-ctx s3bucket s3path i]
  (let [file-name (str s3path "/file_" i)
        input (hdfs/input-stream hdfs-ctx hdfs-file)
        content-len (hdfs/content-length hdfs-ctx hdfs-file)]
    (prn "put file " file-name content-len)

    (s3/stream->s3! s3-ctx input content-len {:bucket s3bucket :file file-name})

    file-name))

(defn hdfs->s3 [hdfs-ctx s3-ctx hdfs-dir s3bucket s3path]
  (loop [i 0 s3files [] hdfs-files (hdfs/list-paths hdfs-ctx hdfs-dir)]

    (if-let [hdfs-file (first hdfs-files)]
      (do
        (prn "uploading " hdfs-file)
        (recur (inc i)
               (conj s3files (hdfs-file->s3 hdfs-ctx hdfs-file s3-ctx s3bucket s3path i))
               (rest hdfs-files)))
      s3files)))

(defn -main [& args]
  (let [[red-url red-user red-pwd red-table s3-access s3-secret region s3bucket s3path hdfs-url hdfs-dir] args]
    (let [s3-ctx (s3/connect! {:access-key s3-access :secret-key s3-secret :region region})
          hdfs-ctx (hdfs/connect! {:default-fs hdfs-url})
          red-ctx (redshift/connect! red-url red-user red-pwd)]

      (hdfs->s3 hdfs-ctx s3-ctx hdfs-dir s3bucket s3path))))



