(ns murmeli.core
  "Murmeli MongoDB driver

  [Javadoc](https://www.mongodb.com/docs/drivers/java/sync/current/)"
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
  "Returns a new unique `org.bson.types.ObjectId` instance."
  []
  (ObjectId/get))

(defn create-id
  "Returns a new unique string representation of an object id."
  []
  (str (create-object-id)))

(defn id?
  "Returns true if given string represents an object id."
  [id]
  (c/id? id))

(defn object-id?
  "Returns true if given object is an `org.bson.types.ObjectId`."
  [id]
  (instance? ObjectId id))

;; Connect and disconnect

(defn connect-client!
  "Connects to a Mongo instance as described by the `db-spec` by constructing a client connection.

  Returns a connection (map).

  Options:
  * `cluster-settings` -- Map of cluster settings, see below
  * `credentials` -- Credentials to use, map of `auth-db`, `username`, and `password`
  * `keywords?` -- If true, deserialize map keys as keywords instead of strings
  * `read-concern` -- Choose level of read isolation, see [[murmeli.data-interop/get-read-concern]]
  * `read-preference` -- Choose preferred replica set members when reading, see [[murmeli.data-interop/get-read-preference]]
  * `retry-reads?` -- Retry reads if they fail due to a network error
  * `retry-writes?` -- Retry writes if they fail due to a network error
  * `ssl-settings` -- Map of SSL settings, see below
  * `uri` -- The connection string to use, eg. \"mongodb://[username:password@]host[:port1],...\"
  * `write-concern` -- Acknowledgement of write operations, see [[murmeli.data-interop/get-write-concern]]

  The `cluster-settings` map:
  * `hosts` -- Sequence of maps with `host` and optionally `port`

  The `ssl-settings` map:
  * `enabled?` -- Enable SSL
  * `invalid-hostname-allowed?` -- Allow invalid hostnames"
  {:arglists '([{:keys [cluster-settings
                        credentials
                        keywords?
                        read-concern
                        read-preference
                        retry-reads?
                        retry-writes?
                        ssl-settings
                        uri
                        write-concern]}])}
  [db-spec]
  (let [settings (di/make-client-settings db-spec)]
    {::client (MongoClients/create settings)}))

(defn connected?
  "Return `true` if the `conn` contains a client connection."
  {:arglists '([conn])}
  [{::keys [client]}]
  (some? client))

(defn disconnect!
  "Disconnect the client."
  {:arglists '([conn])}
  [{::keys [^MongoClient client]}]
  (when client
    (.close client)))

;; Databases

(defn- get-database
  "Find a database by name."
  ^MongoDatabase
  [{::keys [^MongoClient client]}
   database-name]
  {:pre [client database-name]}
  (.getDatabase client database-name))

(defn with-db
  "Retrieve a database using the client and store it in the connection."
  {:arglists '([conn database-name])}
  [{::keys [^MongoDatabase db] :as conn}
   database-name]
  {:pre [database-name]}
  (when-not (connected? conn)
    (throw (ex-info "Cannot retrieve database without a connection" {:database-name database-name})))
  (if-not (and db (= database-name (.getName db)))
    (do
      (log/debugf "Loading database %s" database-name)
      (assoc conn ::db (get-database conn database-name)))
    conn))

(defn list-dbs
  "List all databases as documents.
  Returned documents contain keys like `:name`, `:sizeOnDisk`, `:empty`."
  {:arglists '([conn])}
  [{::keys [^MongoClient client
            ^ClientSession session]}]
  (log/debugf "list databases")
  (let [it (cond
             session (.listDatabases client session PersistentHashMap)
             :else   (.listDatabases client PersistentHashMap))]
    (into [] it)))

(defn drop-db!
  "Drop the given database.
  Does nothing, if the database does not exist. Returns `nil`."
  {:arglists '([conn database-name])}
  [{::keys [^ClientSession session]
    :as    conn}
   database-name]
  {:pre [conn database-name]}
  (let [db (get-database conn database-name)]
    (if session
      (.drop db session)
      (.drop db))))

;; Collections

(defn create-collection!
  "Creates a collection."
  {:arglists '([conn collection])}
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
   [conn collection]
   (get-collection conn collection {}))
  (^MongoCollection
   [{::keys [^MongoDatabase db] :as conn} collection opts]
   {:pre [db collection opts]}
   (let [registry-opts (merge (select-keys conn [:keywords?])
                              opts)]
     (-> db
         (.getCollection (name collection) PersistentHashMap)
         (.withCodecRegistry (c/registry registry-opts))))))

(defn list-collection-names
  "Returns a set of collection names in the current database.

  Options:
  * `batch-size` -- Number of documents per batch
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `keywords?` -- If true, return collection names as keywords"
  {:arglists '([conn & {:keys [batch-size
                               max-time-ms
                               keywords?]}])}
  [{::keys [^MongoDatabase db
            ^ClientSession session]
    :as    conn}
   & {:keys [batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn]}
  (log/debugf "list collection names; %s" options)
  (let [it (cond
             session (.listCollectionNames db session)
             :else   (.listCollectionNames db))]
    ;; TODO: add `filter` parameter
    ;; TODO: add `authorizedCollections` parameter
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (if keywords?
      (into #{} (map keyword) it)
      (into #{} it))))

(defn drop-collection!
  "Drop the given collection from the database"
  {:arglists '([conn collection])}
  [{::keys [^ClientSession session] :as conn}
   collection]
  (let [coll (get-collection conn collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))

;; Transactions / Sessions

(defn with-client-session-options
  "Store session options into `conn`, read by `with-session`"
  {:arglists '([conn & {:keys [causally-consistent?
                               default-timeout-ms
                               read-concern
                               read-preference
                               snapshot?
                               write-concern]}])}
  [conn
   & {:as options}]
  (assoc conn ::session-options (di/make-client-session-options (or options {}))))

(defn- get-session-options
  [conn]
  (or (::session-options conn)
      (throw (ex-info "No session options specified, call `with-client-session-options`"
                      {}))))

(defn- start-session!
  ^ClientSession
  [{::keys [^MongoClient client]}
   session-opts]
  {:pre [client session-opts]}
  (.startSession client session-opts))

(defmacro with-session
  "Run `body` in a session/transaction.

  Gets the session options (as set by `with-client-session-options`), starts a session and stores it
  in `conn` and binds the result to `sym`. The session/transaction is either committed or aborted
  depending on whether `body` throws or not."
  [[sym conn :as bindings] & body]
  {:pre [(vector? bindings)
         (= 2 (count bindings))
         (simple-symbol? sym)]}
  `(let [conn#                   ~conn
         session-opts#           (#'get-session-options conn#)
         ^ClientSession session# (#'start-session! conn# session-opts#)
         ~sym                    (assoc conn# ::session session#)]
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
  "Create a new index."
  {:arglists '([conn
                collection
                index-keys
                & {:keys [background?
                          bits
                          collation-options
                          default-language
                          expire-after-seconds
                          hidden?
                          language-override
                          max-boundary
                          min-boundary
                          name
                          partial-filter-expression
                          sparse?
                          sphere-version
                          storage-engine
                          text-version
                          unique?
                          version
                          weights
                          wildcard-projection]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   index-keys
   & {:as options}]
  {:pre [conn collection (seq index-keys)]}
  (log/debugf "create index; %s %s %s" collection index-keys options)
  (let [coll             (get-collection conn collection)
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
  "List indexes in the given collection."
  {:arglists '([conn
                collection
                & {:keys [batch-size
                          max-time-ms
                          keywords?]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   & {:keys [batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection]}
  (log/debugf "list indexes; %s" (select-keys options [:batch-size
                                                       :max-time-ms
                                                       :keywords?]))
  (let [coll                    (get-collection conn collection {:keywords? keywords?})
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session PersistentHashMap)
                                  (.listIndexes coll PersistentHashMap))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (into [] it)))

(defn drop-all-indexes!
  [{::keys [^ClientSession session] :as conn}
   collection]
  {:pre [conn collection]}
  (let [coll (get-collection conn collection)]
    (if session
      (.dropIndexes coll session)
      (.dropIndexes coll))))

(defn drop-index!
  [{::keys [^ClientSession session] :as conn}
   collection
   index-keys]
  {:pre [conn collection (seq index-keys)]}
  (let [coll       (get-collection conn collection)
        index-keys (di/make-index-bson index-keys)]
    (cond
      session (.dropIndex coll session index-keys)
      :else   (.dropIndex coll index-keys))))

(defn drop-index-by-name!
  [{::keys [^ClientSession session] :as conn}
   collection
   ^String index-name]
  {:pre [conn collection index-name]}
  (let [coll (get-collection conn collection)]
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
  "Insert a single document into a collection
  If the document does not contain an `_id` field, one will be generated (by default an `ObjectId`).
  Returns the `_id` of the inserted document (`String` or `ObjectId`)."
  [{::keys [^ClientSession session] :as conn}
   collection
   doc]
  {:pre [conn collection (map? doc)]}
  (log/debugf "insert one; %s %s" collection (:_id doc))
  (let [coll   (get-collection conn collection)
        ;; TODO: add `InsertOneOptions` support
        result (if session
                 (.insertOne coll session doc)
                 (.insertOne coll doc))]
    (bson-value->document-id (.getInsertedId result))))

(defn insert-many!
  "Insert multiple documents into a collection.
  If the documents do not contain `_id` fields, one will be generated (by default an `ObjectId`).
  Returns the `_id`s of the inserted documents (`String` or `ObjectId`) in the corresponding order."
  [{::keys [^ClientSession session] :as conn}
   collection
   docs]
  {:pre [conn collection (seq docs) (every? map? docs)]}
  (log/debugf "insert many; %s %s" collection (count docs))
  (let [docs   ^List (vec docs)
        coll   (get-collection conn collection)
        ;; TODO: add `InsertManyOptions` support
        result (if session
                 (.insertMany coll session docs)
                 (.insertMany coll docs))]
    (->> (.getInsertedIds result)
         (sort-by key)
         (mapv (comp bson-value->document-id val)))))

;; Updates

(defn update-one!
  "Find document matching `query` and apply `changes` to it.

  Options:
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([conn collection query changes & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (log/debugf "update one; %s %s" collection options)
  (let [coll    (get-collection conn collection)
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
  "Find document(s) matching `query` and apply `changes` to them.

  Options:
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns the number of matched and updated documents."
  {:arglists '([conn collection query changes & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   query
   changes
   & {:as options}]
  {:pre [conn collection (map? query) (map? changes)]}
  (log/debugf "update many; %s %s" collection options)
  (let [coll    (get-collection conn collection)
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
  "Find document(s) matching `query` and replace the first one.
  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([conn collection query changes & {:keys [bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   query
   replacement
   & {:as options}]
  {:pre [conn collection (map? query) (map? replacement)]}
  (log/debugf "replace one; %s %s" collection options)
  (let [coll    (get-collection conn collection)
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
  [{::keys [^ClientSession session] :as conn}
   collection
   & {:keys [query]}]
  {:pre [conn collection (map? query)]}
  (log/debugf "delete one; %s" collection)
  (let [coll   (get-collection conn collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteOne coll session query)
                 :else   (.deleteOne coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

(defn delete-many!
  [{::keys [^ClientSession session] :as conn}
   collection
   & {:keys [query]}]
  {:pre [conn collection (map? query)]}
  (log/debugf "delete many; %s" collection)
  (let [coll   (get-collection conn collection)
        query  (c/map->bson query (.getCodecRegistry coll))
        result (cond
                 session (.deleteMany coll session query)
                 :else   (.deleteMany coll query))]
    {:acknowledged? (.wasAcknowledged result)
     :count         (.getDeletedCount result)}))

;; Queries

(defn count-collection
  "Count the number of documents in a collection"
  {:arglists '([conn collection & {:keys [query
                                          collation-options
                                          comment
                                          hint
                                          limit
                                          max-time-ms
                                          skip]}])}
  [{::keys [^ClientSession session] :as conn}
   collection
   & {:keys [query hint] :as options}]
  {:pre [conn collection]}
  (let [coll     (get-collection conn collection)
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
  [conn
   collection]
  {:pre [conn collection]}
  (-> (get-collection conn collection)
      .estimatedDocumentCount))

(defn find-distinct
  "Find all distinct value of a field in a collection. Returns a set."
  {:arglists '([conn collection field & {:keys [query
                                                batch-size
                                                xform
                                                max-time-ms
                                                keywords?]}])}
  [{::keys [^ClientSession session]
    :as    conn}
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
  (let [coll                 (get-collection conn collection {:keywords? keywords?})
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
  [{::keys [^ClientSession session]
    :as    conn}
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
  (let [coll       (get-collection conn collection {:keywords? keywords?})
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
  "Like `find-all`, but fetches a single document

  By default will warn & throw if query produces more than
  one result."
  {:arglists '([conn collection & {:keys [query
                                          projection
                                          keywords?
                                          warn-on-multiple?
                                          throw-on-multiple?]}])}
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
  "Like `find-one`, but fetches a single document by id."
  {:arglists '([conn collection id & {:keys [projection
                                             keywords?]}])}
  [conn collection id & {:as options}]
  {:pre [conn collection id]}
  (log/debugf "find by id; %s %s %s" collection id (select-keys options [:keywords?]))
  (find-one conn collection (assoc options :query {:_id id})))

;; Find one and - API

(defn find-one-and-delete!
  "Find a document and remove it.
  Returns the document, or ´nil´ if none found."
  {:arglists '([conn collection query & {:keys [collation-options
                                                comment
                                                keywords?
                                                max-time-ms
                                                hint
                                                projection
                                                sort
                                                variables]}])}
  [{::keys [^ClientSession session]
    :as    conn}
   collection
   query
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (seq query)]}
  (log/debugf "find one and delete; %s %s" collection options)
  (let [coll     (get-collection conn collection {:keywords? keywords?})
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
  {:arglists '([conn collection query replacement & {:keys [bypass-validation?
                                                            collation-options
                                                            comment
                                                            hint
                                                            keywords?
                                                            max-time-ms
                                                            projection
                                                            return
                                                            sort
                                                            upsert?
                                                            variables]}])}
  [{::keys [^ClientSession session]
    :as    conn}
   collection
   query
   replacement
   & {:keys [hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (map? replacement) (map? query)]}
  (log/debugf "find one and replace; %s %s" collection options)
  (let [coll     (get-collection conn collection {:keywords? keywords?})
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
  {:arglists '([conn collection query updates & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        keywords?
                                                        max-time-ms
                                                        projection
                                                        return
                                                        sort
                                                        upsert?
                                                        variables]}])}
  [{::keys [^ClientSession session]
    :as    conn}
   collection
   query
   updates
   & {:keys [array-filters hint projection sort variables keywords?]
      :or   {keywords? true}
      :as   options}]
  {:pre [conn collection (map? updates) (map? query)]}
  (log/debugf "find one and update; %s %s" collection options)
  (let [coll     (get-collection conn collection {:keywords? keywords?})
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
    :as    conn}
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
  (let [coll     (get-collection conn collection {:keywords? keywords?})
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
