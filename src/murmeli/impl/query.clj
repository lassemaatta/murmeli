(ns murmeli.impl.query
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.collection :as collection]
            [murmeli.impl.convert :as c]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession DistinctIterable FindIterable]
           [java.util List]
           [java.util.concurrent TimeUnit]
           [org.bson BsonType BsonValue]))

(set! *warn-on-reflection* true)

(defn- bson-value->document-id
  "The inserted ID is either a `BsonObjectId` or `BsonString`"
  [^BsonValue v]
  (let [t (.getBsonType v)]
    (cond
      (= t BsonType/STRING)    (-> v .asString .getValue)
      (= t BsonType/OBJECT_ID) (-> v .asObjectId .getValue))))

;; Insertion

(defn insert-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   doc]
  {:pre [conn collection (map? doc)]}
  (log/debugf "insert one; %s %s" collection (:_id doc))
  (let [coll   (collection/get-collection conn collection)
        ;; TODO: add `InsertOneOptions` support
        result (if session
                 (.insertOne coll session doc)
                 (.insertOne coll doc))]
    (bson-value->document-id (.getInsertedId result))))

(defn insert-many!
  "Insert multiple documents into a collection.
  If the documents do not contain `_id` fields, one will be generated (by default an `ObjectId`).
  Returns the `_id`s of the inserted documents (`String` or `ObjectId`) in the corresponding order."
  [{::session/keys [^ClientSession session] :as conn}
   collection
   docs]
  {:pre [conn collection (seq docs) (every? map? docs)]}
  (log/debugf "insert many; %s %s" collection (count docs))
  (let [docs   ^List (vec docs)
        coll   (collection/get-collection conn collection)
        ;; TODO: add `InsertManyOptions` support
        result (if session
                 (.insertMany coll session docs)
                 (.insertMany coll docs))]
    (->> (.getInsertedIds result)
         (sort-by key)
         (mapv (comp bson-value->document-id val)))))

;; Updates

(defn update-one!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (log/debugf "update one; %s %s" collection options)
  (let [coll    (collection/get-collection conn collection)
        filter  (c/map->bson query (.getCodecRegistry coll))
        updates (c/map->bson changes (.getCodecRegistry coll))
        ;; TODO: array-filters should be BSON
        ;; TODO: variables should be BSON
        options (di/make-update-options (or options {}))
        result  (cond
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
  (log/debugf "update many; %s %s" collection options)
  (let [coll    (collection/get-collection conn collection)
        filter  (c/map->bson query (.getCodecRegistry coll))
        updates (c/map->bson changes (.getCodecRegistry coll))
        ;; TODO: array-filters should be BSON
        ;; TODO: variables should be BSON
        options (di/make-update-options (or options {}))
        result  (cond
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
  (log/debugf "replace one; %s %s" collection options)
  (let [coll    (collection/get-collection conn collection)
        filter  (c/map->bson query (.getCodecRegistry coll))
        options (di/make-replace-options (or options {}))
        result  (cond
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
   query]
  {:pre [conn collection (map? query)]}
  (log/debugf "delete one; %s" collection)
  (let [coll   (collection/get-collection conn collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query]
  {:pre [conn collection (map? query)]}
  (log/debugf "delete many; %s" collection)
  (let [coll   (collection/get-collection conn collection)
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
   & {:keys [query hint] :as options}]
  {:pre [conn collection]}
  (let [coll     (collection/get-collection conn collection)
        registry (.getCodecRegistry coll)
        query    (when query
                   (c/map->bson query registry))
        options  (di/make-count-options
                   (cond-> (or options {})
                     (seq hint) (assoc :hint (c/map->bson hint registry))))]
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

(defn find-distinct
  [{::session/keys [^ClientSession session] :as conn}
   collection
   field
   & {:keys [query
             batch-size
             xform
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection field]}
  (log/debugf "find distinct; %s %s" collection (select-keys options [:keywords? :batch-size :max-time-ms]))
  (let [coll                 (collection/get-collection conn collection {:keywords? keywords?})
        field-name           (name field)
        query                (when (seq query)
                               (c/map->bson query (.getCodecRegistry coll)))
        ^DistinctIterable it (cond
                               ;; We don't know the type of the distinct field so use `Object`
                               (and session query) (.distinct coll session field-name query Object)
                               session             (.distinct coll session field-name Object)
                               query               (.distinct coll field-name query Object)
                               :else               (.distinct coll field-name Object))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (let [res (if xform
                (into #{} xform it)
                (into #{} it))]
      (log/debugf "distinct results: %d" (count res))
      res)))

(defn find-all
  [{::session/keys [^ClientSession session] :as conn}
   collection
   & {:keys [query
             projection
             sort
             xform
             limit
             skip
             batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection]}
  (log/debugf "find all; %s %s" collection (select-keys options [:keywords? :batch-size :max-time-ms :limit :skip]))
  (let [coll       (collection/get-collection conn collection {:keywords? keywords?})
        query      (when (seq query)
                     (c/map->bson query (.getCodecRegistry coll)))
        projection (when (seq projection)
                     (c/map->bson projection (.getCodecRegistry coll)))
        sort       (when (seq sort)
                     (c/map->bson sort (.getCodecRegistry coll)))
        it         ^FindIterable (cond
                                   (and query session) (.find coll session query PersistentHashMap)
                                   session             (.find coll session PersistentHashMap)
                                   query               (.find coll query PersistentHashMap)
                                   :else               (.find coll PersistentHashMap))]
    (when limit (.limit it (int limit)))
    (when skip (.skip it (int skip)))
    (when batch-size (.batchSize it (int batch-size)))
    (when projection (.projection it projection))
    (when sort (.sort it sort))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    ;; Eagerly consume the results, but without chunking
    (let [res (if xform
                (into [] xform it)
                (into [] it))]
      (log/debugf "find all results: %d" (count res))
      res)))

(defn find-one
  [conn
   collection
   & {:keys [warn-on-multiple?
             throw-on-multiple?]
      :or   {warn-on-multiple?  true
             throw-on-multiple? true}
      :as   options}]
  {:pre [conn collection]}
  (log/debugf "find one; %s %s" collection (select-keys options [:keywords? :warn-on-multiple? :throw-on-multiple?]))
  (let [;; "A negative limit is similar to a positive limit but closes the cursor after
        ;; returning a single batch of results."
        ;; https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#negative-values
        cnt       (if (or warn-on-multiple? throw-on-multiple?) -2 -1)
        options   (-> options
                      (select-keys [:query :projection :keywords?])
                      (assoc :limit cnt :batch-size 2))
        results   (find-all conn collection options)
        multiple? (< 1 (count results))]
    (log/debugf "find one results: %d" (count results))
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
  (log/debugf "find by id; %s %s %s" collection id (select-keys options [:keywords?]))
  (find-one conn collection (assoc options :query {:_id id})))

;; Find one and - API

(defn find-one-and-delete!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   query
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (seq query)]}
  (log/debugf "find one and delete; %s %s" collection options)
  (let [coll     (collection/get-collection conn collection {:keywords? keywords?})
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        options  (di/make-find-one-and-delete-options
                   (cond-> (or options {})
                     (seq hint)       (assoc :hint (c/map->bson hint registry))
                     (seq projection) (assoc :projection (c/map->bson projection registry))
                     (seq sort)       (assoc :sort (c/map->bson sort registry))
                     (seq variables)  (assoc :variables (c/map->bson variables registry))))]
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
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (map? replacement) (map? query)]}
  (log/debugf "find one and replace; %s %s" collection options)
  (let [coll     (collection/get-collection conn collection {:keywords? keywords?})
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        options  (di/make-find-one-and-replace-options
                   (cond-> (or options {})
                     (seq hint)       (assoc :hint (c/map->bson hint registry))
                     (seq projection) (assoc :projection (c/map->bson projection registry))
                     (seq sort)       (assoc :sort (c/map->bson sort registry))
                     (seq variables)  (assoc :variables (c/map->bson variables registry))))]
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
   & {:keys [array-filters hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (map? updates) (map? query)]}
  (log/debugf "find one and update; %s %s" collection options)
  (let [coll     (collection/get-collection conn collection {:keywords? keywords?})
        registry (.getCodecRegistry coll)
        query    (c/map->bson query registry)
        updates  (c/map->bson updates registry)
        options  (di/make-find-one-and-update-options
                   (cond-> (or options {})
                     (seq array-filters) (assoc :array-filters (mapv (fn [f] (c/map->bson f registry)) array-filters))
                     (seq hint)          (assoc :hint (c/map->bson hint registry))
                     (seq projection)    (assoc :projection (c/map->bson projection registry))
                     (seq sort)          (assoc :sort (c/map->bson sort registry))
                     (seq variables)     (assoc :variables (c/map->bson variables registry))))]
    (cond
      (and session options) (.findOneAndUpdate coll session query updates options)
      session               (.findOneAndUpdate coll session query updates)
      options               (.findOneAndUpdate coll query updates options)
      :else                 (.findOneAndUpdate coll query updates))))

;; Aggregation

(defn aggregate!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   pipeline
   & {:keys [xform
             batch-size
             max-time-ms
             allow-disk-use?
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (sequential? pipeline)]}
  (log/debugf "aggregate; %s %s" collection
              (select-keys options [:keywords? :allow-disk-use? :batch-size :max-time-ms]))
  (let [coll     (collection/get-collection conn collection {:keywords? keywords?})
        pipeline ^List (mapv (fn [m] (c/map->bson m (.getCodecRegistry coll))) pipeline)
        it       (cond
                   session (.aggregate coll session pipeline)
                   :else   (.aggregate coll pipeline))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (when allow-disk-use? (.allowDiskUse it (boolean allow-disk-use?)))
    (if xform
      (into [] xform it)
      (into [] it))))
