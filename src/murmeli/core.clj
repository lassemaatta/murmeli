(ns murmeli.core
  "https://www.mongodb.com/docs/drivers/java/sync/current/"
  (:require [murmeli.convert :as c])
  (:import [com.mongodb ClientSessionOptions
                        ConnectionString
                        MongoClientSettings
                        ReadConcern
                        ReadPreference
                        ServerApi
                        ServerApiVersion
                        TransactionOptions
                        WriteConcern]
           [com.mongodb.client ClientSession
                               MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase]
           [org.bson BsonDocument]))

(set! *warn-on-reflection* true)

(def ^:private api-version ServerApiVersion/V1)

(defn connect-client!
  [{:keys [^String uri]
    :as   db-spec}]
  {:pre [uri]}
  (let [server-api (-> (ServerApi/builder)
                       (.version api-version)
                       .build)
        settings   (-> (MongoClientSettings/builder)
                       (.applyConnectionString (ConnectionString. uri))
                       (.serverApi server-api)
                       .build)]
    (assoc db-spec ::client (MongoClients/create settings))))

(defn connect-db!
  [{:keys  [^String database]
    ::keys [^MongoClient client]
    :as    db-spec}]
  {:pre [client database]}
  (assoc db-spec ::db (.getDatabase client database)))

;; See https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
(defn make-client-session-options
  ^ClientSessionOptions
  [{:keys [causally-consistent?
           snapshot?
           read-preference
           read-concern
           write-concern]
    :or   {causally-consistent? false
           snapshot?            false}}]
  (-> (ClientSessionOptions/builder)
      (.causallyConsistent causally-consistent?)
      ;; https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/
      (.snapshot snapshot?)
      (.defaultTransactionOptions (-> (TransactionOptions/builder)
                                      (.readPreference (case read-preference
                                                         :nearest             (ReadPreference/nearest)
                                                         :primary             (ReadPreference/primary)
                                                         :secondary           (ReadPreference/secondary)
                                                         :primary-preferred   (ReadPreference/primaryPreferred)
                                                         :secondary-preferred (ReadPreference/secondaryPreferred)
                                                         nil))
                                      (.readConcern (case read-concern
                                                      :available    ReadConcern/AVAILABLE
                                                      :local        ReadConcern/LOCAL
                                                      :linearizable ReadConcern/LINEARIZABLE
                                                      :snapshot     ReadConcern/SNAPSHOT
                                                      :majority     ReadConcern/MAJORITY
                                                      :default      ReadConcern/DEFAULT
                                                      nil))
                                      (.writeConcern (case write-concern
                                                       :w1             WriteConcern/W1
                                                       :w2             WriteConcern/W2
                                                       :w3             WriteConcern/W3
                                                       :majority       WriteConcern/MAJORITY
                                                       :journaled      WriteConcern/JOURNALED
                                                       :acknowledged   WriteConcern/ACKNOWLEDGED
                                                       :unacknowledged WriteConcern/UNACKNOWLEDGED
                                                       nil))
                                      .build))
      .build))

(defn with-client-session-options
  [db-spec
   options]
  (assoc db-spec ::session-options (make-client-session-options options)))

(defn start-session!
  ^ClientSession
  [{::keys [^MongoClient client]}
   session-opts]
  {:pre [client session-opts]}
  (.startSession client session-opts))

(defmacro with-session
  [bindings & body]
  {:pre [(vector? bindings)
         (= 2 (count bindings))
         (simple-symbol? (first bindings))]}
  (let [db-spec-sym (first bindings)
        db-spec     (second bindings)]
    `(let [db-spec#      ~db-spec
           session-opts# (::session-options db-spec#)
           session#      (start-session! db-spec# session-opts#)
           ~db-spec-sym  (assoc db-spec# ::session session#)]
       (try
         (.startTransaction session#)
         (let [result# (do ~@body)]
           (.commitTransaction session#)
           result#)
         (catch Exception e#
           (.abortTransaction session#)
           (throw e#))))))

(defn disconnect!
  [{::keys [^MongoClient client]
    :as    db-spec}]
  (.close client)
  (dissoc db-spec ::client ::database))

(defn- get-collection
  ^MongoCollection
  [{::keys [^MongoDatabase db]} collection]
  {:pre [db collection]}
  (.getCollection db (name collection) BsonDocument))

(defn insert-one
  [{::keys [^ClientSession session] :as db-spec}
   collection
   doc]
  {:pre [collection doc]}
  (let [bson   (c/to-bson doc)
        coll   (get-collection db-spec collection)
        result (if session
                 (.insertOne coll session bson)
                 (.insertOne coll bson))]
    (.. result getInsertedId asObjectId getValue toHexString)))

(defn count-collection
  [{::keys [^ClientSession session] :as db-spec} collection]
  (let [coll (get-collection db-spec collection)]
    (if session
      (.countDocuments coll session)
      (.countDocuments coll))))
