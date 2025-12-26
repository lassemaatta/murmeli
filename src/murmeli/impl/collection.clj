(ns murmeli.impl.collection
  "Collection implementation.

  See [MongoCollection](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoCollection.html)."
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.client :as client]
            [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.db :as db])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession
                               DistinctIterable
                               FindIterable
                               ListIndexesIterable]
           [com.mongodb.client.model IndexOptions]
           [java.util List]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

;;; Collection

(defn drop-collection!
  [{::client/keys [^ClientSession session] :as conn}
   collection]
  (let [coll (db/get-collection conn collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))

;;; Index

(defn create-index!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   index-keys
   & {:keys [partial-filter-expression
             storage-engine
             weights]
      :as   options}]
  {:pre [conn collection (map? index-keys)]}
  (let [coll             (db/get-collection conn collection)
        registry         (.getCodecRegistry coll)
        index-keys       (di/make-index-bson index-keys)
        ^IndexOptions io (cond-> options
                           partial-filter-expression (update :partial-filter-expression c/map->bson registry)
                           storage-engine            (update :storage-engine c/map->bson registry)
                           weights                   (update :weights c/map->bson registry)
                           (seq options)             (di/make-index-options))]
    (cond
      (and io session) (.createIndex coll session index-keys io)
      session          (.createIndex coll session index-keys)
      io               (.createIndex coll index-keys io)
      :else            (.createIndex coll index-keys))))

(defn list-indexes
  [{::client/keys [^ClientSession session] :as conn}
   collection
   & {:keys [batch-size
             max-time-ms]}]
  {:pre [conn collection]}
  (let [coll                    (db/get-collection conn collection)
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session PersistentHashMap)
                                  (.listIndexes coll PersistentHashMap))
        it                      (cond-> it
                                  batch-size  (.batchSize (int batch-size))
                                  max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))]
    (into [] (cursor/->reducible it))))

(defn drop-all-indexes!
  [{::client/keys [^ClientSession session] :as conn}
   collection]
  {:pre [conn collection]}
  (let [coll (db/get-collection conn collection)]
    (if session
      (.dropIndexes coll session)
      (.dropIndexes coll))))

(defn drop-index!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   index-keys]
  {:pre [conn collection (seq index-keys)]}
  (let [coll       (db/get-collection conn collection)
        index-keys (di/make-index-bson index-keys)]
    (cond
      session (.dropIndex coll session index-keys)
      :else   (.dropIndex coll index-keys))))

(defn drop-index-by-name!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   ^String index-name]
  {:pre [conn collection index-name]}
  (let [coll (db/get-collection conn collection)]
    (cond
      session (.dropIndex coll session index-name)
      :else   (.dropIndex coll index-name))))

;;; Helpers

(defn- preprocess-projection
  [projection registry]
  (when (seq projection)
    (cond-> projection
      (sequential? projection) (zipmap (repeat 1))
      true                     (c/map->bson registry))))

(defn- preprocess-options
  "Several API methods have similar options which require similar preprocessing"
  [{:keys [array-filters
           hint
           projection
           sort
           variables]
    :as   options}
   registry]
  (cond-> (or options {})
    (seq array-filters) (assoc :array-filters (mapv (fn [f] (c/map->bson f registry)) array-filters))
    (seq hint)          (assoc :hint (c/map->bson hint registry))
    (seq projection)    (assoc :projection (preprocess-projection projection registry))
    (seq sort)          (assoc :sort (di/make-sort sort))
    (seq variables)     (assoc :variables (c/map->bson variables registry))
    true                not-empty))

;; Insertion

(defn insert-one!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   doc
   & {:as options}]
  {:pre [conn collection (map? doc)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-insert-one-options)
        result   (cond
                   (and session options) (.insertOne coll session doc options)
                   session               (.insertOne coll session doc)
                   options               (.insertOne coll doc options)
                   :else                 (.insertOne coll doc))
        id       (.getInsertedId result)]
    (cond-> {:acknowledged? (.wasAcknowledged result)}
      id (assoc :_id (c/bson-value->document-id id)))))

(defn insert-many!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   docs
   & {:as options}]
  {:pre [conn collection (seq docs) (every? map? docs)]}
  (let [docs     ^List (vec docs)
        coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-insert-many-options)
        result   (cond
                   (and session options) (.insertMany coll session docs options)
                   session               (.insertMany coll session docs)
                   options               (.insertMany coll docs options)
                   :else                 (.insertMany coll docs))]
    {:acknowledged? (.wasAcknowledged result)
     :_ids          (->> (.getInsertedIds result)
                          (filter identity)
                          (sort-by key)
                          (mapv (comp c/bson-value->document-id val)))}))

;; Updates

(defn update-one!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        filter   (c/map->bson query registry)
        updates  (c/map->bson changes registry)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-update-options)
        result   (cond
                   (and session options) (.updateOne coll session filter updates options)
                   session               (.updateOne coll session filter updates)
                   options               (.updateOne coll filter updates options)
                   :else                 (.updateOne coll filter updates))]
    (di/update-result->map result)))

(defn update-many!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        filter   (c/map->bson query (.getCodecRegistry coll))
        updates  (c/map->bson changes (.getCodecRegistry coll))
        options  (some-> options
                         (preprocess-options registry)
                         di/make-update-options)
        result   (cond
                   (and session options) (.updateMany coll session filter updates options)
                   session               (.updateMany coll session filter updates)
                   options               (.updateMany coll filter updates options)
                   :else                 (.updateMany coll filter updates))]
    (di/update-result->map result)))

;; Replace

(defn replace-one!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   replacement
   & {:as options}]
  {:pre [conn collection (map? query) (map? replacement)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        filter   (c/map->bson query (.getCodecRegistry coll))
        options  (some-> options
                         (preprocess-options registry)
                         di/make-replace-options)
        result   (cond
                   (and session options) (.replaceOne coll session filter replacement options)
                   session               (.replaceOne coll session filter replacement)
                   options               (.replaceOne coll filter replacement options)
                   :else                 (.replaceOne coll filter replacement))]
    (di/update-result->map result)))

;; Deletes

(defn delete-one!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query]
  {:pre [conn collection (map? query)]}
  (let [coll   (db/get-collection conn collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query]
  {:pre [conn collection (map? query)]}
  (let [coll   (db/get-collection conn collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteMany coll session query)
                 :else   (.deleteMany coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

;; Queries

(defn count-collection
  [{::client/keys [^ClientSession session] :as conn}
   collection
   & {:keys [query] :as options}]
  {:pre [conn collection]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        query    (when query
                   (c/map->bson query registry))
        options  (some-> options
                         (preprocess-options registry)
                         di/make-count-options)]
    (cond
      (and session query options) (.countDocuments coll session query options)
      (and session query)         (.countDocuments coll session query)
      session                     (.countDocuments coll session)
      (and query options)         (.countDocuments coll query options)
      query                       (.countDocuments coll query)
      :else                       (.countDocuments coll))))

(defn estimated-count-collection
  [conn
   collection]
  {:pre [conn collection]}
  (-> (db/get-collection conn collection)
      .estimatedDocumentCount))

(defn find-distinct-reducible
  [{::client/keys [^ClientSession session] :as conn}
   collection
   field
   & {:keys [query
             batch-size
             max-time-ms]}]
  {:pre [conn collection field]}
  (let [coll                 (db/get-collection conn collection)
        field-name           (name field)
        query                (when (seq query)
                               (c/map->bson query (.getCodecRegistry coll)))
        ^DistinctIterable it (cond
                               ;; We don't know the type of the distinct field so use `Object`
                               (and session query) (.distinct coll session field-name query Object)
                               session             (.distinct coll session field-name Object)
                               query               (.distinct coll field-name query Object)
                               :else               (.distinct coll field-name Object))
        it                   (cond-> it
                               batch-size  (.batchSize (int batch-size))
                               max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))]
    (cursor/->reducible it)))

(defn find-reducible
  [{::client/keys [^ClientSession session] :as conn}
   collection
   & {:keys [batch-size
             limit
             max-time-ms
             projection
             query
             skip
             sort]}]
  {:pre [conn collection]}
  (let [coll       (db/get-collection conn collection)
        registry   (.getCodecRegistry coll)
        query      (when (seq query)
                     (c/map->bson query registry))
        projection (preprocess-projection projection registry)
        sort       (when (seq sort)
                     (di/make-sort sort))
        it         ^FindIterable (cond
                                   (and query session) (.find coll session query PersistentHashMap)
                                   session             (.find coll session PersistentHashMap)
                                   query               (.find coll query PersistentHashMap)
                                   :else               (.find coll PersistentHashMap))
        it         (cond-> it
                     limit       (.limit (int limit))
                     skip        (.skip (int skip))
                     batch-size  (.batchSize (int batch-size))
                     projection  (.projection projection)
                     sort        (.sort sort)
                     max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))]
    (cursor/->reducible it)))

(defn find-one
  [conn
   collection
   & {:keys [warn-on-multiple?
             throw-on-multiple?]
      :or   {warn-on-multiple?  true
             throw-on-multiple? true}
      :as   options}]
  {:pre [conn collection]}
  (let [;; "A negative limit is similar to a positive limit but closes the cursor after
        ;; returning a single batch of results."
        ;; https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#negative-values
        cnt       (if (or warn-on-multiple? throw-on-multiple?) -2 -1)
        options   (-> options
                      (select-keys #{:query :projection})
                      (assoc :limit cnt :batch-size 2))
        results   (into [] (find-reducible conn collection options))
        multiple? (< 1 (count results))]
    ;; Check if the query really did produce a single result, or did we (accidentally?)
    ;; match multiple documents?
    (when (and multiple? warn-on-multiple?)
      (log/warn "find-one found multiple results"))
    (when (and multiple? throw-on-multiple?)
      (throw (ex-info "find-one found multiple results" {:collection collection})))
    (first results)))

(defn find-by-id
  [conn collection id & {:as options}]
  {:pre [conn collection id]}
  (find-one conn collection (assoc options :query {:_id id})))

;; Find one and - API

(defn find-one-and-delete!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   & {:as options}]
  {:pre [conn collection (seq query)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-find-one-and-delete-options)]
    (cond
      (and session options) (.findOneAndDelete coll session query options)
      session               (.findOneAndDelete coll session query)
      options               (.findOneAndDelete coll query options)
      :else                 (.findOneAndDelete coll query))))

(defn find-one-and-replace!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   replacement
   & {:as options}]
  {:pre [conn collection (map? replacement) (map? query)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-find-one-and-replace-options)]
    (cond
      (and session options) (.findOneAndReplace coll session query replacement options)
      session               (.findOneAndReplace coll session query replacement)
      options               (.findOneAndReplace coll query replacement options)
      :else                 (.findOneAndReplace coll query replacement))))

(defn find-one-and-update!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   query
   updates
   & {:as options}]
  {:pre [conn collection (map? updates) (map? query)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        updates  (c/map->bson updates registry)
        options  (some-> options
                         (preprocess-options registry)
                         di/make-find-one-and-update-options)]
    (cond
      (and session options) (.findOneAndUpdate coll session query updates options)
      session               (.findOneAndUpdate coll session query updates)
      options               (.findOneAndUpdate coll query updates options)
      :else                 (.findOneAndUpdate coll query updates))))

;; Aggregation

(defn aggregate-reducible!
  [{::client/keys [^ClientSession session] :as conn}
   collection
   pipeline
   & {:keys [allow-disk-use?
             batch-size
             max-time-ms]}]
  {:pre [conn collection (sequential? pipeline)]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        pipeline ^List (mapv (fn [m] (c/map->bson m registry)) pipeline)
        it       (cond
                   session (.aggregate coll session pipeline)
                   :else   (.aggregate coll pipeline))
        it       (cond-> it
                   batch-size      (.batchSize (int batch-size))
                   max-time-ms     (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
                   allow-disk-use? (.allowDiskUse (boolean allow-disk-use?)))]
    (cursor/->reducible it)))

;; Change streams

(defn watch
  [{::client/keys [^ClientSession session] :as conn}
   collection
   & {:keys [batch-size
             collation-options
             ^String comment
             full-document
             full-document-before-change
             max-time-ms
             pipeline]}]
  {:pre [conn collection]}
  (let [coll     (db/get-collection conn collection)
        registry (.getCodecRegistry coll)
        pipeline (when (seq pipeline)
                   ^List (mapv (fn [m] (c/map->bson m registry)) pipeline))
        it       (cond
                   (and session pipeline) (.watch coll session pipeline PersistentHashMap)
                   session                (.watch coll PersistentHashMap)
                   pipeline               (.watch coll pipeline PersistentHashMap)
                   :else                  (.watch coll PersistentHashMap))
        it       (cond-> it
                   collation-options           (.collation (di/make-collation collation-options))
                   comment                     (.comment comment)
                   batch-size                  (.batchSize (int batch-size))
                   full-document               (.fullDocument (di/get-full-document full-document))
                   full-document-before-change (.fullDocumentBeforeChange (di/get-full-document-before-change full-document-before-change))
                   max-time-ms                 (.maxAwaitTime (long max-time-ms) TimeUnit/MILLISECONDS))
        csd->clj (map (fn [csd]
                        (di/change-stream-document csd (fn [b] (c/bson-document->map b registry)))))]
    (->> (cursor/->reducible-cs it)
         (eduction csd->clj))))
