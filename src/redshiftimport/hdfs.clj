(ns
  ^{:doc "Reading and providing different inputstreams for files in a directory or single files
          supports globs"}
  redshiftimport.hdfs
  (:import [org.apache.hadoop.fs FileSystem Path FileStatus]
           [org.apache.hadoop.conf Configuration]
           [java.io InputStream File]))


(defrecord Ctx [^FileSystem fs])

(defn status->path
  "Transforms a FileStatus to a Path"
  [^FileStatus status]
  (.getPath status))

(defn file? [^FileStatus status]
  (.isFile status))

(defn ^Path as-path
  "Ensure that the file is a Path if its a String a Path is created from it"
  [file]
  (if (instance? Path file)
    file
    (Path. (str file))))

(defn file-name [file]
  (.getName (File. (.toString file))))

(defn ^InputStream input-stream
  "Return an InputStream from the file"
  [{:keys [^FileSystem fs]} file]
  {:pre [fs file]}
  (.open fs (as-path file)))

(defn content-length [{:keys [^FileSystem fs]} file]
  {:pre [fs file]}
  (.getLen (.getFileStatus fs (as-path file))))


(defn list-paths
  "Only return files, as Path objects"
  [{:keys [fs]} glob]
  {:pre [fs glob]}
  (map status->path (filter file? (.globStatus ^FileSystem fs (Path. (str glob))))))

(defn connect! [{:keys [default-fs hdfs-impl] :or {hdfs-impl "org.apache.hadoop.hdfs.DistributedFileSystem"}}]
  {:pre [default-fs hdfs-impl]}
  (->Ctx (FileSystem/get (doto (Configuration.)
                           (.set "fs.defaultFS" (str default-fs))
                           (.set "fs.AbstractFileSystem.hdfs.impl" (str hdfs-impl))))))