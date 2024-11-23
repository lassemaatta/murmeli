(ns murmeli.core
  "https://www.mongodb.com/docs/drivers/java/sync/current/"
  (:require [clojure.tools.logging :as log]
            [murmeli.convert :as c]
            [murmeli.cursor]
            [murmeli.data-interop :as di])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession
                               DistinctIterable
                               FindIterable
                               ListIndexesIterable
                               MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase]
           [com.mongodb.client.model IndexOptions]
           [java.util List]
           [java.util.concurrent TimeUnit]
           [org.bson BsonType BsonValue]
           [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

;; Utilities

(defn create-object-id
  "Returns a new object-id"
  []
  (ObjectId/get))

(defn create-id
  "Returns a new object-id as string"
  []
  (str (create-object-id)))

(defn id?
  "Return true if given string represents an object-id"
  [id]
  (c/id? id))

(defn object-id?
  [id]
  (instance? ObjectId id))

;; Connect and disconnect

(defn connect-client!
  "Connect to a Mongo instance and construct a Client"
  {:arglists '([{:keys [uri
                        credentials
                        ssl-settings
                        cluster-settings
                        read-concern
                        write-concern
                        read-preference
                        retry-reads?
                        retry-writes?
                        keywords?]}])}
  [db-spec]
  (let [settings (di/make-client-settings db-spec)]
    (-> db-spec
        (assoc ::client (MongoClients/create settings))
        (dissoc ::db))))

(defn connected?
  [{::keys [client]}]
  (some? client))

(defn disconnect!
  "Disconnect the Client and discard any related state"
  [{::keys [^MongoClient client]
    :as    db-spec}]
  (when client
    (.close client))
  (->> db-spec
       (filter (fn [[k _]]
                 (simple-keyword? k)))
       (into {})))

;; Databases

(defn- get-database
  "Find a database by name"
  ^MongoDatabase
  [{::keys [^MongoClient client]}
   database-name]
  {:pre [client database-name]}
  (.getDatabase client database-name))

(defn with-db
  "Retrieve a database using the client and store it in `db-spec`."
  ([{:keys [database-name]
     :as   db-spec}]
   (with-db db-spec database-name))
  ([{::keys [^MongoDatabase db] :as db-spec}
    database-name]
   {:pre [database-name]}
   (when-not (connected? db-spec)
     (throw (ex-info "Cannot retrieve database without a connection" {:database-name database-name})))
   (if-not (and db (= database-name (.getName db)))
     (do
       (log/debugf "Loading database %s" database-name)
       (assoc db-spec ::db (get-database db-spec database-name)))
     db-spec)))

(defn list-dbs
  "List all databases"
  [{::keys [^MongoClient client
            ^ClientSession session]}]
  (log/debugf "list databases")
  (let [it (cond
             session (.listDatabases client session PersistentHashMap)
             :else   (.listDatabases client PersistentHashMap))]
    (into [] it)))

(defn drop-db!
  "Drop the given database. Does nothing, if the database does not exist. Returns `nil`."
  {:arglists '([db-spec database-name])}
  [{::keys [^ClientSession session]
    :as    db-spec}
   database-name]
  {:pre [db-spec database-name]}
  (let [db (get-database db-spec database-name)]
    (if session
      (.drop db session)
      (.drop db))))

;; Collections

(defn create-collection!
  "Creates a collection"
  [{::keys [^MongoDatabase db
            ^ClientSession session]}
   collection]
  {:pre [db collection]}
  ;; TODO: Add support for `CreateCollectionOptions`
  (cond
    session (.createCollection db session (name collection))
    :else   (.createCollection db (name collection))))

(defn- get-collection
  (^MongoCollection
   [db-spec collection]
   (get-collection db-spec collection {}))
  (^MongoCollection
   [{::keys [^MongoDatabase db] :as db-spec} collection opts]
   {:pre [db collection opts]}
   (let [registry-opts (merge (select-keys db-spec [:keywords?])
                              opts)]
     (-> db
         (.getCollection (name collection) PersistentHashMap)
         (.withCodecRegistry (c/registry registry-opts))))))

(defn list-collection-names
  [{::keys [^MongoDatabase db
            ^ClientSession session]
    :as    db-spec}
   & {:keys [batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec]}
  (log/debugf "list collection names; %s" (select-keys options [:keywords?]))
  (let [it (cond
             session (.listCollectionNames db session)
             :else   (.listCollectionNames db))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (into #{} (map (if keywords? keyword identity)) it)))

(defn drop-collection!
  [{::keys [^ClientSession session] :as db-spec}
   collection]
  (let [coll (get-collection db-spec collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))

;; Transactions / Sessions

(defn with-client-session-options
  "Store session options into `db-spec`, read by `with-session`"
  {:arglists '([db-spec & {:keys [causally-consistent?
                                  snapshot?
                                  read-preference
                                  read-concern
                                  write-concern]}])}
  [db-spec
   & {:as options}]
  (assoc db-spec ::session-options (di/make-client-session-options (or options {}))))

(defn- get-session-options
  [db-spec]
  (or (::session-options db-spec)
      (throw (ex-info "No session options specified, call `with-client-session-options`"
                      {}))))

(defn- start-session!
  ^ClientSession
  [{::keys [^MongoClient client]}
   session-opts]
  {:pre [client session-opts]}
  (.startSession client session-opts))

(defmacro with-session
  "Run `body` in a session/transaction

  Gets the session options (as set by `with-client-session-options`), starts a session and stores it
  in `db-spec` and binds the result to `sym`. The session/transaction is either committed or aborted
  depending on whether `body` throws or not."
  [[sym db-spec :as bindings] & body]
  {:pre [(vector? bindings)
         (= 2 (count bindings))
         (simple-symbol? sym)]}
  `(let [db-spec#                ~db-spec
         session-opts#           (#'get-session-options db-spec#)
         ^ClientSession session# (#'start-session! db-spec# session-opts#)
         ~sym                    (assoc db-spec# ::session session#)]
     (try
       (.startTransaction session#)
       (let [result# (do ~@body)]
         (.commitTransaction session#)
         result#)
       (catch Exception e#
         (.abortTransaction session#)
         (throw e#)))))

;; Indexes

(defn create-index!
  "Create a new index"
  {:arglists '([db-spec
                collection
                index-keys
                & {:keys [background?
                          bits
                          default-language
                          expire-after-seconds
                          name
                          partial-filter-expression
                          sparse?
                          unique?
                          version]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   index-keys
   & {:as options}]
  {:pre [db-spec collection (seq index-keys)]}
  (log/debugf "create index; %s %s %s" collection index-keys (select-keys options [:background?
                                                                                   :bits
                                                                                   :default-language
                                                                                   :expire-after-seconds
                                                                                   :name
                                                                                   :partial-filter-expression
                                                                                   :sparse?
                                                                                   :unique?
                                                                                   :version]))
  (let [coll             (get-collection db-spec collection)
        index-keys       (di/make-index-bson index-keys)
        ^IndexOptions io (cond-> options
                           (:partial-filter-expression options)
                           (update :partial-filter-expression (fn [pfe] (c/map->bson pfe (.getCodecRegistry coll))))
                           (seq options)
                           (di/make-index-options))]
    (cond
      (and io session) (.createIndex coll session index-keys io)
      session          (.createIndex coll session index-keys)
      io               (.createIndex coll index-keys io)
      :else            (.createIndex coll index-keys))))

(defn list-indexes
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection]}
  (log/debugf "list indexes; %s" (select-keys options [:batch-size
                                                       :max-time-ms
                                                       :keywords?]))
  (let [coll                    (get-collection db-spec collection {:keywords? keywords?})
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session PersistentHashMap)
                                  (.listIndexes coll PersistentHashMap))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (into [] it)))

(defn drop-all-indexes!
  [{::keys [^ClientSession session] :as db-spec}
   collection]
  {:pre [db-spec collection]}
  (let [coll (get-collection db-spec collection)]
    (if session
      (.dropIndexes coll session)
      (.dropIndexes coll))))

(defn drop-index!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   index-keys]
  {:pre [db-spec collection (seq index-keys)]}
  (let [coll       (get-collection db-spec collection)
        index-keys (di/make-index-bson index-keys)]
    (cond
      session (.dropIndex coll session index-keys)
      :else   (.dropIndex coll index-keys))))

(defn drop-index-by-name!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   ^String index-name]
  {:pre [db-spec collection index-name]}
  (let [coll (get-collection db-spec collection)]
    (cond
      session (.dropIndex coll session index-name)
      :else   (.dropIndex coll index-name))))

;; Insertion

(defn- bson-value->document-id
  "The inserted ID is either a `BsonObjectId` or `BsonString`"
  [^BsonValue v]
  (let [t (.getBsonType v)]
    (cond
      (= t BsonType/STRING)    (-> v .asString .getValue)
      (= t BsonType/OBJECT_ID) (-> v .asObjectId .getValue))))

(defn insert-one!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   doc]
  {:pre [db-spec collection (map? doc)]}
  (log/debugf "insert one; %s %s" collection (:_id doc))
  (let [coll   (get-collection db-spec collection)
        result (if session
                 (.insertOne coll session doc)
                 (.insertOne coll doc))]
    (bson-value->document-id (.getInsertedId result))))

(defn insert-many!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   docs]
  {:pre [db-spec collection (seq docs) (every? map? docs)]}
  (log/debugf "insert many; %s %s" collection (count docs))
  (let [bsons  ^List (mapv identity #_c/to-bson docs)
        coll   (get-collection db-spec collection)
        result (if session
                 (.insertMany coll session bsons)
                 (.insertMany coll bsons))]
    (->> (.getInsertedIds result)
         (sort-by key)
         (mapv (comp bson-value->document-id val)))))

;; Updates

(defn update-one!
  "Find document(s) matching `query` and update the first one.
  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([db-spec collection query changes & {:keys [upsert?]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   query
   changes
   & {:as options}]
  {:pre [db-spec collection (map? query) (map? changes)]}
  (log/debugf "update one; %s %s" collection (select-keys options [:upsert?]))
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query (.getCodecRegistry coll))
        updates (c/map->bson changes (.getCodecRegistry coll))
        options (di/make-update-options (or options {}))
        result  (cond
                  (and session options) (.updateOne coll session filter updates options)
                  session               (.updateOne coll session filter updates)
                  options               (.updateOne coll filter updates options)
                  :else                 (.updateOne coll filter updates))]
    ;; There doesn't seem to be a way to verify that the query would match
    ;; just a single document because matched count is always either 0 or 1 :(
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

(defn update-many!
  "Find document(s) matching `query` and update them.
  Returns the number of matched and updated documents."
  {:arglists '([db-spec collection query changes & {:keys [upsert?]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   query
   changes
   & {:as options}]
  {:pre [db-spec collection (map? query) (map? changes)]}
  (log/debugf "update many; %s %s" collection (select-keys options [:upsert?]))
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query (.getCodecRegistry coll))
        updates (c/map->bson changes (.getCodecRegistry coll))
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
  "Find document(s) matching `query` and replace the first one.
  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([db-spec collection query changes & {:keys [upsert?]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   query
   replacement
   & {:as options}]
  {:pre [db-spec collection (map? query) (map? replacement)]}
  (log/debugf "replace one; %s %s" collection (select-keys options [:upsert?]))
  (let [coll    (get-collection db-spec collection)
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
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query]}]
  {:pre [db-spec collection (map? query)]}
  (log/debugf "delete one; %s" collection)
  (let [coll   (get-collection db-spec collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query]}]
  {:pre [db-spec collection (map? query)]}
  (log/debugf "delete many; %s" collection)
  (let [coll   (get-collection db-spec collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteMany coll session query)
                 :else   (.deleteMany coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

;; Queries

(defn count-collection
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query hint] :as options}]
  {:pre [db-spec collection]}
  (let [coll     (get-collection db-spec collection)
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
  "Gets an estimate of the count of documents in a collection using collection metadata."
  [db-spec
   collection]
  {:pre [db-spec collection]}
  (-> (get-collection db-spec collection)
      .estimatedDocumentCount))

(defn find-distinct
  "Find all distinct value of a field in a collection. Returns a set."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   field
   & {:keys [query
             batch-size
             xform
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection field]}
  (log/debugf "find distinct; %s %s" collection (select-keys options [:keywords? :batch-size :max-time-ms]))
  (let [xform                (or xform (map identity))
        coll                 (get-collection db-spec collection {:keywords? keywords?})
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
    (let [res (transduce xform conj #{} it)]
      (log/debugf "distinct results: %d" (count res))
      res)))

(defn find-all
  [{::keys [^ClientSession session]
    :as    db-spec}
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
  {:pre [db-spec collection]}
  (log/debugf "find all; %s %s" collection (select-keys options [:keywords? :batch-size :max-time-ms :limit :skip]))
  (let [coll       (get-collection db-spec collection {:keywords? keywords?})
        query      (when (seq query)
                     (c/map->bson query (.getCodecRegistry coll)))
        projection (when (seq projection)
                     (c/map->bson projection (.getCodecRegistry coll)))
        sort       (when (seq sort)
                     (c/map->bson sort (.getCodecRegistry coll)))
        xform      (or xform (map identity))
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
    (let [res (transduce xform conj it)]
      (log/debugf "find all results: %d" (count res))
      res)))

(defn find-one
  "Like `find-all`, but fetches a single document

  By default will warn & throw if query produces more than
  one result."
  {:arglists '([db-spec collection & {:keys [query
                                             projection
                                             keywords?
                                             warn-on-multiple?
                                             throw-on-multiple?]}])}
  [db-spec
   collection
   & {:keys [warn-on-multiple?
             throw-on-multiple?]
      :or   {warn-on-multiple?  true
             throw-on-multiple? true}
      :as   options}]
  {:pre [db-spec collection]}
  (log/debugf "find one; %s %s" collection (select-keys options [:keywords? :warn-on-multiple? :throw-on-multiple?]))
  (let [;; "A negative limit is similar to a positive limit but closes the cursor after
        ;; returning a single batch of results."
        ;; https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#negative-values
        cnt       (if (or warn-on-multiple? throw-on-multiple?) -2 -1)
        options   (-> options
                      (select-keys [:query :projection :keywords?])
                      (assoc :limit cnt :batch-size 2))
        results   (find-all db-spec collection options)
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
  "Like `find-one`, but fetches a single document by id."
  {:arglists '([db-spec collection id & {:keys [projection
                                                keywords?]}])}
  [db-spec collection id & {:as options}]
  {:pre [db-spec collection id]}
  (log/debugf "find by id; %s %s %s" collection id (select-keys options [:keywords?]))
  (find-one db-spec collection (assoc options :query {:_id id})))

;; Find one and - API

(defn find-one-and-delete!
  "Find a document and remove it.
  Returns the document, or ´nil´ if none found."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection (seq query)]}
  (log/debugf "find one and delete; %s %s" collection (select-keys options [:keywords?]))
  (let [coll     (get-collection db-spec collection {:keywords? keywords?})
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
  "Find a document and replace it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   replacement
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection (map? replacement) (map? query)]}
  (log/debugf "find one and replace; %s %s" collection
              (select-keys options [:keywords? :upsert? :return]))
  (let [coll     (get-collection db-spec collection {:keywords? keywords?})
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
  "Find a document and update it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   updates
   & {:keys [array-filters hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection (map? updates) (map? query)]}
  (log/debugf "find one and update; %s %s" collection
              (select-keys options [:keywords? :upsert? :return]))
  (let [coll     (get-collection db-spec collection {:keywords? keywords?})
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
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   pipeline
   & {:keys [xform
             batch-size
             max-time-ms
             allow-disk-use?
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [db-spec collection (sequential? pipeline)]}
  (log/debugf "aggregate; %s %s" collection
              (select-keys options [:keywords? :allow-disk-use? :batch-size :max-time-ms]))
  (let [coll     (get-collection db-spec collection {:keywords? keywords?})
        pipeline ^List (mapv (fn [m] (c/map->bson m (.getCodecRegistry coll))) pipeline)
        it       (cond
                   session (.aggregate coll session pipeline)
                   :else   (.aggregate coll pipeline))
        xform    (or xform (map identity))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (when allow-disk-use? (.allowDiskUse it (boolean allow-disk-use?)))
    (transduce xform conj it)))
