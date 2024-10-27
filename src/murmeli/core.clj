(ns murmeli.core
  "https://www.mongodb.com/docs/drivers/java/sync/current/"
  (:require [clojure.tools.logging :as log]
            [murmeli.convert :as c]
            [murmeli.cursor]
            [murmeli.data-interop :as di])
  (:import [com.mongodb.client ClientSession
                               DistinctIterable
                               FindIterable
                               ListIndexesIterable
                               MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase]
           [java.util List]
           [java.util.concurrent TimeUnit]
           [org.bson BsonDocument BsonValue]
           [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

(defn- bson->clj-xform
  "Transforms BSON documents to clojure maps, optionally with map keys as keywords"
  [keywords?]
  (map (partial c/from-bson {:keywords? keywords?})))

;; Utilities

(defn create-id
  "Returns a new object-id"
  []
  (str (ObjectId/get)))

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
                        retry-writes?]}])}
  [db-spec]
  (let [settings (di/make-client-settings db-spec)]
    (assoc db-spec ::client (MongoClients/create settings))))

(defn connected?
  [{::keys [client]}]
  (some? client))

(defn disconnect!
  "Disconnect the Client and discard any related state"
  [{::keys [^MongoClient client]
    :as    db-spec}]
  (.close client)
  (select-keys db-spec [:uri]))

;; Databases

(defn- get-database
  "Find a database by name"
  ^MongoDatabase
  [{::keys [^MongoClient client]}
   database-name]
  {:pre [client database-name]}
  (.getDatabase client database-name))

(defn with-db
  "Find a database and store it in `db-spec`."
  ([{:keys [database-name]
     :as   db-spec}]
   (with-db db-spec database-name))
  ([db-spec
    database-name]
   {:pre [database-name]}
   (assoc db-spec ::db (get-database db-spec database-name))))

(defn list-dbs
  "List all databases"
  [{::keys [^MongoClient client
            ^ClientSession session]}]
  (let [it (cond
             session (.listDatabases client session BsonDocument)
             :else   (.listDatabases client BsonDocument))]
    (transduce (bson->clj-xform true) conj it)))

(defn drop-db!
  "Drop a database"
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
  ^MongoCollection
  [{::keys [^MongoDatabase db]} collection]
  {:pre [db collection]}
  (.getCollection db (name collection) BsonDocument))

(defn list-collection-names
  [{::keys [^MongoDatabase db
            ^ClientSession session]
    :as    db-spec}
   & {:keys [batch-size
             max-time-ms
             keyword?]
      :or   {keyword? true}}]
  {:pre [db-spec]}
  (let [it (cond
             session (.listCollectionNames db session)
             :else   (.listCollectionNames db))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (into #{} (map (if keyword? keyword identity)) it)))

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
                & {:keys [background
                          name
                          version
                          partial-filter-expression
                          unique?
                          sparse?]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   index-keys
   & {:as options}]
  {:pre [db-spec collection (seq index-keys)]}
  (let [coll       (get-collection db-spec collection)
        index-keys (di/make-index-bson index-keys)
        io         (when options
                     (di/make-index-options options))]
    (cond
      (and io session) (.createIndex coll session index-keys io)
      session          (.createIndex coll session index-keys)
      io               (.createIndex coll index-keys io)
      :else            (.createIndex coll index-keys))))

(defn list-indexes
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [batch-size
             max-time-ms]}]
  {:pre [db-spec collection]}
  (let [coll                    (get-collection db-spec collection)
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session BsonDocument)
                                  (.listIndexes coll BsonDocument))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (transduce (bson->clj-xform true) conj it)))

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
  [^BsonValue v]
  (c/from-bson v))

(defn insert-one!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   doc]
  {:pre [db-spec collection doc]}
  (let [bson   (c/to-bson doc)
        coll   (get-collection db-spec collection)
        result (if session
                 (.insertOne coll session bson)
                 (.insertOne coll bson))]
    (bson-value->document-id (.getInsertedId result))))

(defn insert-many!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   docs]
  {:pre [db-spec collection docs]}
  (let [bsons  ^List (mapv c/to-bson docs)
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
  {:pre [db-spec collection query changes]}
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query)
        updates (c/map->bson changes)
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
  {:pre [db-spec collection query changes]}
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query)
        updates (c/map->bson changes)
        options (di/make-update-options (or options {}))
        result  (cond
                  (and session options) (.updateMany coll session filter updates options)
                  session               (.updateMany coll session filter updates)
                  options               (.updateMany coll filter updates options)
                  :else                 (.updateMany coll filter updates))]
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

;; Deletes

(defn delete-one!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query]}]
  {:pre [db-spec collection query]}
  (let [coll   (get-collection db-spec collection)
        query  (c/map->bson query)
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query]}]
  {:pre [db-spec collection query]}
  (let [coll   (get-collection db-spec collection)
        query  (c/map->bson query)
        result (cond
                 session (.deleteMany coll session query)
                 :else   (.deleteMany coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

;; Queries

(defn count-collection
  [{::keys [^ClientSession session] :as db-spec}
   collection
   & {:keys [query] :as options}]
  {:pre [db-spec collection]}
  (let [coll    (get-collection db-spec collection)
        query   (when query
                  (c/map->bson query))
        options (di/make-count-options options)]
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
      :or   {keywords? true}}]
  {:pre [db-spec collection field]}
  (let [xform-clj            (bson->clj-xform keywords?)
        xform                (if xform (comp xform-clj xform) xform-clj)
        coll                 (get-collection db-spec collection)
        field-name           (name field)
        query                (when (seq query)
                               (c/map->bson query))
        ^DistinctIterable it (cond
                               (and session query) (.distinct coll session field-name query BsonValue)
                               session             (.distinct coll session field-name BsonValue)
                               query               (.distinct coll field-name query BsonValue)
                               :else               (.distinct coll field-name BsonValue))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (transduce xform conj #{} it)))

(defn- projection-keys->bson
  [projection]
  (->> projection
       (mapcat (fn [field-name]
                 [field-name 1]))
       (apply array-map)
       c/map->bson))

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
      :or   {keywords? true}}]
  {:pre [db-spec collection]}
  (let [xform-clj  (bson->clj-xform keywords?)
        xform      (if xform (comp xform-clj xform) xform-clj)
        coll       (get-collection db-spec collection)
        query      (when (seq query)
                     (c/map->bson query))
        projection (when (seq projection)
                     (projection-keys->bson projection))
        sort       (when (seq sort)
                     (c/map->bson sort))
        it         ^FindIterable (cond
                                   (and query session) (.find coll session query)
                                   session             (.find coll session)
                                   query               (.find coll query)
                                   :else               (.find coll))]
    (when limit (.limit it (int limit)))
    (when skip (.skip it (int skip)))
    (when batch-size (.batchSize it (int batch-size)))
    (when projection (.projection it projection))
    (when sort (.sort it sort))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    ;; Eagerly consume the results, but without chunking
    (transduce xform conj it)))

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
  (let [;; "A negative limit is similar to a positive limit but closes the cursor after
        ;; returning a single batch of results."
        ;; https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#negative-values
        cnt       (if (or warn-on-multiple? throw-on-multiple?) -2 -1)
        options   (-> options
                      (select-keys [:query :projection :keywords?])
                      (assoc :limit cnt :batch-size 2))
        results   (find-all db-spec collection options)
        multiple? (< 1 (count results))]
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
  (find-one db-spec collection (assoc options :query {:_id id})))

;; Find one and - API

(defn find-one-and-delete!
  "Find a document and remove it.
  Returns the document, or ´nil´ if none found."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   & {:keys [projection sort keywords?]
      :or   {keywords? true}}]
  {:pre [db-spec collection (seq query)]}
  (let [query   (c/map->bson query)
        options (-> {:sort       (when (seq sort)
                                   (c/map->bson sort))
                     :projection (when (seq projection)
                                   (projection-keys->bson projection))}
                    di/make-find-one-and-delete-options)
        coll    (get-collection db-spec collection)
        result  (cond
                  (and session options) (.findOneAndDelete coll session query options)
                  session               (.findOneAndDelete coll session query)
                  options               (.findOneAndDelete coll query options)
                  :else                 (.findOneAndDelete coll query))]
    (some->> result
             (c/from-bson {:keywords? keywords?}))))

(defn find-one-and-replace!
  "Find a document and replace it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   replacement
   & {:keys [projection sort return upsert? keywords?]
      :or   {keywords? true
             return    :after}}]
  {:pre [db-spec collection (seq replacement) (seq query)]}
  (let [query       (c/map->bson query)
        replacement (c/to-bson replacement)
        options     (-> {:projection (when (seq projection)
                                       (projection-keys->bson projection))
                         :return     return
                         :sort       (when (seq sort)
                                       (c/map->bson sort))
                         :upsert?    upsert?}
                        di/make-find-one-and-replace-options)
        coll        (get-collection db-spec collection)
        result      (cond
                      (and session options) (.findOneAndReplace coll session query replacement options)
                      session               (.findOneAndReplace coll session query replacement)
                      options               (.findOneAndReplace coll query replacement options)
                      :else                 (.findOneAndReplace coll query replacement))]
    (some->> result
             (c/from-bson {:keywords? keywords?}))))

(defn find-one-and-update!
  "Find a document and update it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  [{::keys [^ClientSession session]
    :as    db-spec}
   collection
   query
   updates
   & {:keys [projection sort return upsert? keywords?]
      :or   {keywords? true
             return    :after}}]
  {:pre [db-spec collection (seq updates) (seq query)]}
  (let [query   (c/map->bson query)
        updates (c/map->bson updates)
        options (-> {:projection (when (seq projection)
                                   (projection-keys->bson projection))
                     :sort       sort
                     :return     return
                     :upsert?    upsert?}
                    di/make-find-one-and-update-options)
        coll    (get-collection db-spec collection)
        result  (cond
                  (and session options) (.findOneAndUpdate coll session query updates options)
                  session               (.findOneAndUpdate coll session query updates)
                  options               (.findOneAndUpdate coll query updates options)
                  :else                 (.findOneAndUpdate coll query updates))]
    (some->> result
             (c/from-bson {:keywords? keywords?}))))

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
      :or   {keywords? true}}]
  {:pre [db-spec collection (sequential? pipeline)]}
  (let [coll      (get-collection db-spec collection)
        pipeline  ^List (mapv c/map->bson pipeline)
        it        (cond
                    session (.aggregate coll session pipeline)
                    :else   (.aggregate coll pipeline))
        xform-clj (bson->clj-xform keywords?)
        xform     (if xform (comp xform-clj xform) xform-clj)]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (when allow-disk-use? (.allowDiskUse it (boolean allow-disk-use?)))
    (transduce xform conj it)))
