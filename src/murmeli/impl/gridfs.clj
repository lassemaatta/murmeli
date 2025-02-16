(ns murmeli.impl.gridfs
  "GridFS implementation"
  {:no-doc true}
  (:refer-clojure :exclude [find])
  (:require [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.db :as db]
            [murmeli.impl.session :as session])
  (:import [com.mongodb.client ClientSession MongoDatabase]
           [com.mongodb.client.gridfs GridFSBucket GridFSBuckets]
           [com.mongodb.client.gridfs.model GridFSFile GridFSUploadOptions]
           [java.io InputStream]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(defn create-bucket
  ([conn]
   (create-bucket conn nil))
  ([{::db/keys [^MongoDatabase db]}
    ^String bucket-name
    & {:keys [chunk-size-bytes
              read-concern
              read-preference
              timeout-ms]}]
   (cond-> (if bucket-name
             (GridFSBuckets/create db bucket-name)
             (GridFSBuckets/create db))
     chunk-size-bytes (.withChunkSizeBytes chunk-size-bytes)
     read-concern     (.withReadConcern (di/get-read-concern read-concern))
     read-preference  (.withReadPreference (di/get-read-preference read-preference))
     timeout-ms       (.withTimeout timeout-ms TimeUnit/MILLISECONDS))))

(defn with-bucket
  ([conn]
   (with-bucket conn nil))
  ([conn bucket-name & {:as options}]
   (assoc conn ::bucket (create-bucket conn bucket-name options))))

(defn drop-bucket!
  ([{::keys [bucket] :as conn}]
   (drop-bucket! conn bucket))
  ([{::session/keys [^ClientSession session] :as conn}
    bucket-or-bucket-name]
   {:pre [conn bucket-or-bucket-name]}
   (let [^GridFSBucket bucket (cond
                                ;; Bucket name given?
                                (string? bucket-or-bucket-name)
                                (create-bucket conn bucket-or-bucket-name)
                                ;; Bucket instance given?
                                (instance? GridFSBucket bucket-or-bucket-name)
                                bucket-or-bucket-name
                                :else
                                (throw (ex-info "Not a bucker nor a bucket name"
                                                {:bucket bucket-or-bucket-name})))]
     (cond
       session (.drop bucket session)
       :else   (.drop bucket))
     bucket)))

(defn upload-stream!
  [{::session/keys [^ClientSession session]
    ::keys         [^GridFSBucket bucket]
    :as            conn}
   ^String filename
   ^InputStream source
   & {:keys [id doc]
      :as   options}]
  {:pre [conn bucket filename source]}
  (let [registry                     (db/registry conn)
        id                           (some-> id c/->object-id c/object-id->bson)
        ^GridFSUploadOptions options (when (seq options)
                                       (cond-> options
                                         doc  (update :doc c/map->document registry)
                                         true di/make-gridfs-upload-options))
        new-id                       (cond
                                       ;; Return void
                                       (and session id options) (.uploadFromStream bucket session id filename source options)
                                       (and session id)         (.uploadFromStream bucket session id filename source)
                                       (and id options)         (.uploadFromStream bucket id filename source options)
                                       id                       (.uploadFromStream bucket id filename source)
                                       ;; Return the generated ID
                                       (and session options)    (.uploadFromStream bucket session filename source options)
                                       session                  (.uploadFromStream bucket session filename source)
                                       options                  (.uploadFromStream bucket filename source options)
                                       :else                    (.uploadFromStream bucket filename source))]
    (or new-id id)))

(defn gfs-file->map
  [registry ^GridFSFile f]
  (let [doc (.getMetadata f)]
    (cond-> {:_id         (.getObjectId f)
             :chunk-size  (.getChunkSize f)
             :filename    (.getFilename f)
             :length      (.getLength f)
             :upload-date (.getUploadDate f)}
      doc (assoc :doc (c/document->map doc registry)))))

(defn download-stream
  [{::session/keys [^ClientSession session]
    ::keys         [^GridFSBucket bucket]
    :as            conn}
   & {:keys [id ^String filename revision] :as options}]
  {:pre [conn bucket (or id filename)]}
  (let [registry (db/registry conn)
        id       (some-> id c/->object-id c/object-id->bson)
        options  (when revision
                   (di/make-gridfs-download-options options))
        stream   (cond
                   (and session filename options) (.openDownloadStream bucket session filename options)
                   (and session filename)         (.openDownloadStream bucket session filename)
                   (and session id)               (.openDownloadStream bucket session id)
                   (and filename options)         (.openDownloadStream bucket filename options)
                   filename                       (.openDownloadStream bucket filename)
                   id                             (.openDownloadStream bucket id))]
    {:input-stream stream
     :file         (gfs-file->map registry (.getGridFSFile stream))}))

(defn find
  [{::session/keys [^ClientSession session]
    ::keys         [^GridFSBucket bucket]
    :as            conn}
   & {:keys [batch-size
             collation-options
             limit
             max-time-ms
             query
             skip
             sort]}]
  {:pre [conn bucket]}
  (let [registry  (db/registry conn)
        query     (when query
                    (c/map->bson query registry))
        collation (when collation-options
                    (di/make-collation collation-options))
        it        (cond
                    (and session query) (.find bucket session query)
                    session             (.find bucket session)
                    query               (.find bucket query)
                    :else               (.find bucket))
        it        (cond-> it
                    batch-size  (.batchSize (int batch-size))
                    collation   (.collation collation)
                    limit       (.limit (int limit))
                    max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
                    skip        (.skip (int skip))
                    sort        (.sort (c/map->bson sort registry)))]
    (->> (cursor/->reducible it)
         (eduction (map (partial gfs-file->map registry))))))

(defn delete!
  [{::session/keys [^ClientSession session]
    ::keys         [^GridFSBucket bucket]}
   id]
  {:pre [bucket id]}
  (let [id (c/->object-id id)]
    (cond
      session (.delete bucket session id)
      :else   (.delete bucket id))))

(defn rename!
  [{::session/keys [^ClientSession session]
    ::keys         [^GridFSBucket bucket]}
   id
   ^String new-filename]
  {:pre [bucket id new-filename]}
  (let [id (c/->object-id id)]
    (cond
      session (.rename bucket session id new-filename)
      :else   (.rename bucket id new-filename))))
