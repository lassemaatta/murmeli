(ns murmeli.impl.query
  "Query implementation"
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.collection :as collection]
            [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession DistinctIterable FindIterable]
           [java.util List]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

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
    (seq sort)          (assoc :sort (c/map->bson sort registry))
    (seq variables)     (assoc :variables (c/map->bson variables registry))
    true                not-empty))

;; Insertion

(defn insert-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   doc
   & {:as options}]
  {:pre [conn collection (map? doc)]}
  (let [coll     (collection/get-collection conn collection options)
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
      id (assoc :id (c/bson-value->document-id id)))))

(defn insert-many!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   docs
   & {:as options}]
  {:pre [conn collection (seq docs) (every? map? docs)]}
  (let [docs     ^List (vec docs)
        coll     (collection/get-collection conn collection options)
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
     :ids           (->> (.getInsertedIds result)
                         (filter identity)
                         (sort-by key)
                         (mapv (comp c/bson-value->document-id val)))}))

;; Updates

(defn update-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (let [coll     (collection/get-collection conn collection options)
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
    ;; There doesn't seem to be a way to verify that the query would match
    ;; just a single document because matched count is always either 0 or 1 :(
    ;; TODO: support `getUpsertedId`
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

(defn update-many!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (let [coll     (collection/get-collection conn collection options)
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
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

;; Replace

(defn replace-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   replacement
   & {:as options}]
  {:pre [conn collection (map? query) (map? replacement)]}
  (let [coll     (collection/get-collection conn collection options)
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
    ;; There doesn't seem to be a way to verify that the query would match
    ;; just a single document because matched count is always either 0 or 1 :(
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

;; Deletes

(defn delete-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   & {:as options}]
  {:pre [conn collection (map? query)]}
  (let [coll   (collection/get-collection conn collection options)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   & {:as options}]
  {:pre [conn collection (map? query)]}
  (let [coll   (collection/get-collection conn collection options)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteMany coll session query)
                 :else   (.deleteMany coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

;; Queries

(defn count-collection
  [{::session/keys [^ClientSession session] :as conn}
   collection
   & {:keys [query] :as options}]
  {:pre [conn collection]}
  (let [coll     (collection/get-collection conn collection options)
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
  (-> (collection/get-collection conn collection)
      .estimatedDocumentCount))

(defn find-distinct-reducible
  [{::session/keys [^ClientSession session] :as conn}
   collection
   field
   & {:keys [query
             batch-size
             max-time-ms]
      :as   options}]
  {:pre [conn collection field]}
  (let [coll                 (collection/get-collection conn collection options)
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
  [{::session/keys [^ClientSession session] :as conn}
   collection
   & {:keys [batch-size
             limit
             max-time-ms
             projection
             query
             skip
             sort]
      :as   options}]
  {:pre [conn collection]}
  (let [coll       (collection/get-collection conn collection options)
        registry   (.getCodecRegistry coll)
        query      (when (seq query)
                     (c/map->bson query registry))
        projection (preprocess-projection projection registry)
        sort       (when (seq sort)
                     (c/map->bson sort registry))
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
                      (select-keys [:query :projection :keywords? :allow-qualified?])
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
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   & {:as options}]
  {:pre [conn collection (seq query)]}
  (let [coll     (collection/get-collection conn collection options)
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
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   replacement
   & {:as options}]
  {:pre [conn collection (map? replacement) (map? query)]}
  (let [coll     (collection/get-collection conn collection options)
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
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   updates
   & {:as options}]
  {:pre [conn collection (map? updates) (map? query)]}
  (let [coll     (collection/get-collection conn collection options)
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
  [{::session/keys [^ClientSession session] :as conn}
   collection
   pipeline
   & {:keys [allow-disk-use?
             batch-size
             max-time-ms]
      :as   options}]
  {:pre [conn collection (sequential? pipeline)]}
  (let [coll     (collection/get-collection conn collection options)
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
