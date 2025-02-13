(ns murmeli.gridfs
  (:refer-clojure :exclude [find])
  (:require [murmeli.impl.gridfs :as gfs]))

(defn create-bucket
  {:arglists '([conn]
               [conn & {:keys [chunk-size-bytes
                               read-concern
                               read-preference
                               timeout-ms]}])}
  ([conn]
   (create-bucket conn nil))
  ([conn
    bucket-name
    & {:as options}]
   (gfs/create-bucket conn bucket-name options)))

(defn with-bucket
  {:arglists '([conn]
               [conn & {:keys [chunk-size-bytes
                               read-concern
                               read-preference
                               timeout-ms]}])}
  ([conn]
   (gfs/with-bucket conn nil))
  ([conn bucket-name & {:as options}]
   (gfs/with-bucket conn bucket-name options)))

(defn drop-bucket!
  [conn]
  (gfs/drop-bucket! conn))

(defn upload-stream!
  {:arglists '([conn filename source & {:keys [id
                                               doc
                                               chunk-size-bytes]}])}
  [conn filename source & {:as options}]
  (gfs/upload-stream! conn filename source options))

(defn download-stream
  {:arglists '([conn & {:keys [id
                               filename
                               revision]}])}
  [conn & {:as options}]
  (gfs/download-stream conn options))

(defn find
  {:arglists '([conn & {:keys [batch-size
                               collation-options
                               limit
                               max-time-ms
                               query
                               skip
                               sort]}])}
  [conn & {:as options}]
  (gfs/find conn options))

(defn delete!
  [conn id]
  (gfs/delete! conn id))

(defn rename!
  [conn id new-filename]
  (gfs/rename! conn id new-filename))
