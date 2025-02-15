(ns murmeli.gridfs
  (:refer-clojure :exclude [find])
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.gridfs :as gfs])
  (:import [com.mongodb.client.gridfs GridFSBucket]))

(set! *warn-on-reflection* true)

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
   (let [bucket (gfs/create-bucket conn bucket-name options)]
     (log/debugf "Created bucket '%s'" bucket-name)
     bucket)))

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

;; TODO: should have bucket parameter
(defn drop-bucket!
  [conn]
  (gfs/drop-bucket! conn)
  (log/debugf "Dropped bucket '%s'" (.getBucketName ^GridFSBucket (::gfs/bucket conn))))

(defn upload-stream!
  {:arglists '([conn filename source & {:keys [id
                                               doc
                                               chunk-size-bytes]}])}
  [conn filename source & {:as options}]
  (let [id (gfs/upload-stream! conn filename source options)]
    (log/debugf "Uploaded '%s' with id '%s" filename id)
    id))

(defn download-stream
  {:arglists '([conn & {:keys [id
                               filename
                               revision]}])}
  [conn & {:as options}]
  (let [response (gfs/download-stream conn options)]
    (log/debugf "Downloaded '%s'" (-> response :file :_id))
    response))

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
  (gfs/delete! conn id)
  (log/debugf "Deleted file '%s'" id))

(defn rename!
  [conn id new-filename]
  (gfs/rename! conn id new-filename)
  (log/debugf "Renamed file '%s' to '%s'" id new-filename))
