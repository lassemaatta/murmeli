(ns murmeli.core
  (:require [murmeli.convert :as c])
  (:import [com.mongodb ClientSessionOptions
                        ConnectionString
                        MongoClientSettings
                        ServerApi
                        ServerApiVersion]
           [com.mongodb.client MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase]
           [org.bson BsonDocument]))

(set! *warn-on-reflection* true)

(def ^:private api-version ServerApiVersion/V1)

(defn connect-client!
  [{:keys [^String uri]
    :as   ^MongoClients ctx}]
  {:pre [uri]}
  (let [server-api (-> (ServerApi/builder)
                       (.version api-version)
                       .build)
        settings   (-> (MongoClientSettings/builder)
                       (.applyConnectionString (ConnectionString. uri))
                       (.serverApi server-api)
                       .build)]
    (assoc ctx ::client (MongoClients/create settings))))

(defn connect-db!
  [{:keys  [^String database]
    ::keys [^MongoClient client]
    :as    ctx}]
  {:pre [client database]}
  (assoc ctx ::db (.getDatabase client database)))

;; See https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
(def default-client-session-options
  (-> (ClientSessionOptions/builder)
      (.causallyConsistent false)
      ;; https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/
      (.snapshot false)
      .build))

;; TODO: implement
(defmacro with-session
  [ctx & body]
  `(let []
     (try
       (do
         ~@body)
       (catch Exception e#
         (throw e#))
       (finally
         (.end s#)))))

(defn disconnect!
  [{::keys [^MongoClient client] :as ctx}]
  (.close client)
  (dissoc ctx ::client ::database))

(defn- get-collection
  ^MongoCollection [{::keys [^MongoDatabase db]} collection]
  {:pre [db collection]}
  (.getCollection db (name collection) BsonDocument))

(defn insert-one
  [ctx collection doc]
  {:pre [collection doc]}
  (let [bson   (c/to-bson doc)
        coll   (get-collection ctx collection)
        result (.insertOne coll bson)]
    (.. result getInsertedId asObjectId getValue toHexString)))

(defn count-collection
  [ctx collection]
  (let [coll (get-collection ctx collection)]
    (.countDocuments coll)))
