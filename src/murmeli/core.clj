(ns murmeli.core
  "https://www.mongodb.com/docs/drivers/java/sync/current/"
  (:require [clojure.tools.logging :as log]
            [murmeli.convert :as c]
            [murmeli.cursor]
            [murmeli.data-interop :as di])
  (:import [com.mongodb.client ClientSession
                               FindIterable
                               ListIndexesIterable
                               MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase]
           [java.util List]
           [org.bson BsonDocument BsonValue]))

(set! *warn-on-reflection* true)

(defn- bson->clj-xform
  "Transforms BSON documents to clojure maps, optionally with map keys as keywords"
  [keywords?]
  (map (partial c/from-bson {:keywords? keywords?})))

;; Connect and disconnect

(defn connect-client!
  "Connect to a Mongo instance and construct a Client"
  [db-spec]
  (let [settings (di/make-client-settings db-spec)]
    (assoc db-spec ::client (MongoClients/create settings))))

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
  (let [db (get-database db-spec database-name)]
    (if session
      (.drop db session)
      (.drop db))))

;; Collections

(defn create-collection
  "Creates a collection"
  [{::keys [^MongoDatabase db
            ^ClientSession session]}
   collection]
  ;; TODO: Add support for `CreateCollectionOptions`
  (cond
    session (.createCollection db session (name collection))
    :else   (.createCollection db (name collection))))

(defn- get-collection
  ^MongoCollection
  [{::keys [^MongoDatabase db]} collection]
  {:pre [db collection]}
  (.getCollection db (name collection) BsonDocument))

;; Transactions / Sessions

(defn with-client-session-options
  "Store session options into `db-spec`, read by `with-session`"
  {:arglists '([db-spec & {:keys [causally-consistent?
                                  snapshot?
                                  read-preference
                                  read-concern
                                  write-concern]
                           :or   {causally-consistent? false
                                  snapshot?            false}}])}
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
                keys
                & {:keys [background
                          name
                          version
                          unique?
                          sparse?]}])}
  [{::keys [^ClientSession session] :as db-spec}
   collection
   keys
   & {:as options}]
  (let [coll (get-collection db-spec collection)
        keys (di/make-index-bson keys)
        io   (when options
               (di/make-index-options options))]
    (cond
      (and io session) (.createIndex coll session keys io)
      session          (.createIndex coll session keys)
      io               (.createIndex coll keys io)
      :else            (.createIndex coll keys))))

(defn list-indexes
  [{::keys [^ClientSession session] :as db-spec}
   collection]
  (let [coll                    (get-collection db-spec collection)
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session BsonDocument)
                                  (.listIndexes coll BsonDocument))]
    (transduce (bson->clj-xform true) conj it)))

(defn drop-all-indexes!
  [{::keys [^ClientSession session] :as db-spec}
   collection]
  (let [coll (get-collection db-spec collection)]
    (if session
      (.dropIndexes coll session)
      (.dropIndexes coll))))

(defn drop-index!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   keys]
  (let [coll (get-collection db-spec collection)
        keys (di/make-index-bson keys)]
    (cond
      session (.dropIndex coll session keys)
      :else   (.dropIndex coll keys))))

(defn drop-index-by-name!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   ^String index-name]
  (let [coll (get-collection db-spec collection)]
    (cond
      session (.dropIndex coll session index-name)
      :else   (.dropIndex coll index-name))))

;; Insertion

(defn- bson-value->object-id
  [^BsonValue v]
  (.. v asObjectId getValue toHexString))

(defn insert-one!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   doc]
  {:pre [collection doc]}
  (let [bson   (c/to-bson doc)
        coll   (get-collection db-spec collection)
        result (if session
                 (.insertOne coll session bson)
                 (.insertOne coll bson))]
    (bson-value->object-id (.getInsertedId result))))

(defn insert-many!
  [{::keys [^ClientSession session] :as db-spec}
   collection
   docs]
  {:pre [collection docs]}
  (let [bsons  ^List (mapv c/to-bson docs)
        coll   (get-collection db-spec collection)
        result (if session
                 (.insertMany coll session bsons)
                 (.insertMany coll bsons))]
    (->> (.getInsertedIds result)
         (sort-by key)
         (mapv (comp bson-value->object-id val)))))

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
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query)
        updates ^List (mapv c/map->bson changes)
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
  (let [coll    (get-collection db-spec collection)
        filter  (c/map->bson query)
        updates ^List (mapv c/map->bson changes)
        options (di/make-update-options (or options {}))
        result  (cond
                  (and session options) (.updateMany coll session filter updates options)
                  session               (.updateMany coll session filter updates)
                  options               (.updateMany coll filter updates options)
                  :else                 (.updateMany coll filter updates))]
    {:modified (.getModifiedCount result)
     :matched  (.getMatchedCount result)}))

;; Queries

(defn count-collection
  ([db-spec
    collection]
   (count-collection db-spec collection {}))
  ([{::keys [^ClientSession session] :as db-spec}
    collection
    query]
   (let [coll   (get-collection db-spec collection)
         filter (c/map->bson query)]
     (cond
       (and session filter) (.countDocuments coll session filter)
       session              (.countDocuments coll session)
       filter               (.countDocuments coll filter)
       :else                (.countDocuments coll)))))

(defn estimated-count-collection
  "Gets an estimate of the count of documents in a collection using collection metadata."
  [db-spec
   collection]
  (-> (get-collection db-spec collection)
      .estimatedDocumentCount))

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
             keywords?]
      :or   {keywords? true}}]
  (let [xform-clj  (bson->clj-xform keywords?)
        xform      (if xform (comp xform-clj xform) xform-clj)
        coll       (get-collection db-spec collection)
        query      (when (seq query)
                     (c/map->bson query))
        projection (when (seq projection)
                     (->> projection
                          (mapcat (fn [field-name]
                                    [field-name 1]))
                          (apply array-map)
                          c/map->bson))
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
    ;; Eagerly consume the results, but without chunking
    (transduce xform conj it)))

(defn find-one
  "Like `find-all`, but fetches a single document

  By default will warn & throw if query produces more than
  one result."
  [db-spec
   collection
   & {:keys [warn-on-multiple?
             throw-on-multiple?]
      :or   {warn-on-multiple?  true
             throw-on-multiple? true}
      :as   options}]
  (let [;; "A negative limit is similar to a positive limit but closes the cursor after
        ;; returning a single batch of results."
        ;; https://www.mongodb.com/docs/manual/reference/method/cursor.limit/#negative-values
        cnt       (if (or warn-on-multiple? throw-on-multiple?) -2 -1)
        options   (assoc options :limit cnt :batch-size 2)
        results   (find-all db-spec collection options)
        multiple? (< 1 (count results))]
    ;; Check if the query really did produce a single result, or did we (accidentally?)
    ;; match multiple documents?
    (when (and multiple? warn-on-multiple?)
      (log/warn "find-one found multiple results"))
    (when (and multiple? throw-on-multiple?)
      (throw (ex-info "find-one found multiple results" {:collection collection})))
    (first results)))
