(ns
  ^{:doc "Reading and providing different inputstreams for files in a directory or single files
          supports globs"}
  redshiftimport.hdfs
  (:import [org.apache.hadoop.fs FileSystem Path FileStatus]
           [org.apache.hadoop.conf Configuration]))


(defrecord Ctx [^FileSystem fs])

(defn status->path [^FileStatus status]
  (.getPath status))

(defn file? [^FileStatus status]
  (.isFile status))

(defn input-stream [{:keys [^FileSystem fs]} file]
  {:pre [fs (string? file)]}
  (.open fs (str file)))

(defn content-length [{:keys [^FileSystem fs]} file]
  {:pre [fs (string? file)]}
  (.getLen (.getFileStatus fs (str file))))


(defn list-paths
  "Only return files"
  [{:keys [fs]} glob]
  {:pre [fs glob]}
  (map status->path (filter file? (.globStatus ^FileSystem fs (Path. (str glob))))))

(defn connect! [{:keys [default-fs hdfs-impl] :or {hdfs-impl "org.apache.hadoop.hdfs.DistributedFileSystem"}}]
  {:pre [default-fs hdfs-impl]}
  (->Ctx (FileSystem/get (doto (Configuration.)
                           (.set "fs.defaultFS" (str default-fs))
                           (.set "fs.AbstractFileSystem.hdfs.impl" (str hdfs-impl))))))