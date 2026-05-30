(ns murmeli.impl.db
  "Database implementation.

  See [MongoDatabase](https://mongodb.github.io/mongo-java-driver/5.8/apidocs/driver-sync/com/mongodb/client/MongoDatabase.html)."
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.client :as client]
            [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession MongoCollection MongoDatabase]
           [com.mongodb.client.model CreateCollectionOptions]
           [java.util.concurrent TimeUnit]
           [org.bson.codecs.configuration CodecRegistry]))

(set! *warn-on-reflection* true)

;;; Database

(defn with-db
  [{::keys [^MongoDatabase db] :as conn}
   database-name]
  {:pre [database-name]}
  (when-not (client/connected? conn)
    (throw (ex-info "Cannot retrieve database without a connection" {:database-name database-name})))
  (if-not (and db (= database-name (.getName db)))
    (do
      (log/debugf "Loading database %s" database-name)
      (assoc conn ::db (client/get-database conn database-name)))
    conn))

(defn get-registry
  ^CodecRegistry [{::keys [^MongoDatabase db]}]
  (.getCodecRegistry db))

(defn with-registry
  [{::keys [^MongoDatabase db] :as conn}
   ^CodecRegistry registry]
  {:pre [conn db registry]}
  (if (= registry (get-registry conn))
    conn
    (assoc conn ::db (.withCodecRegistry db registry))))

(defn drop-db!
  [{::client/keys [^ClientSession session]
    :as           conn}
   database-name]
  {:pre [conn database-name]}
  (let [db (client/get-database conn database-name)]
    (if session
      (.drop db session)
      (.drop db))))

(defn run-command!
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]}
   command
   & {:keys [read-preference]}]
  (let [registry  (.getCodecRegistry db)
        bson      (c/map->bson command registry)
        read-pref (some-> read-preference di/read-preference->java)]
    (cond
      (and session
           read-pref) (.runCommand db session bson read-pref PersistentHashMap)
      session         (.runCommand db session bson PersistentHashMap)
      read-pref       (.runCommand db bson read-pref PersistentHashMap)
      :else           (.runCommand db bson PersistentHashMap))))

;;; Collection

(defn create-collection!
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]
    :as           conn}
   collection
   & {:keys [encrypted-fields
             storage-engine-options]
      :as   options}]
  {:pre [db collection]}
  (let [registry                         (get-registry conn)
        ^CreateCollectionOptions options (when (seq options)
                                           (cond-> options
                                             encrypted-fields       (update :encrypted-fields c/map->bson registry)
                                             storage-engine-options (update :storage-engine-options c/map->bson registry)
                                             true                   di/make-create-collection-options))]
    (cond
      (and session options) (.createCollection db session (name collection) options)
      session               (.createCollection db session (name collection))
      options               (.createCollection db (name collection) options)
      :else                 (.createCollection db (name collection)))))

(defn get-collection
  ^MongoCollection
  [{::keys [^MongoDatabase db]} collection]
  {:pre [db collection]}
  (.getCollection db (name collection) PersistentHashMap))

(defn list-collection-names-reducible
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]
    :as           conn}
   & {:keys [authorized-collections?
             batch-size
             ^String comment
             keywords?
             max-time-ms
             query]
      :or   {keywords? true}}]
  {:pre [conn db]}
  (let [registry (.getCodecRegistry db)
        it       (cond
                   session (.listCollectionNames db session)
                   :else   (.listCollectionNames db))
        it       (cond-> it
                   authorized-collections? (.authorizedCollections (boolean authorized-collections?))
                   batch-size              (.batchSize (int batch-size))
                   comment                 (.comment comment)
                   query                   (.filter (c/map->bson query registry))
                   max-time-ms             (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))]
    (cond->> (cursor/->reducible it)
      keywords? (eduction (map keyword)))))

(defn list-collections-reducible
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]
    :as           conn}
   & {:keys [batch-size
             ^String comment
             max-time-ms
             query
             timeout-mode]}]
  {:pre [conn db]}
  (let [registry (.getCodecRegistry db)
        it       (cond
                   session (.listCollections db session PersistentHashMap)
                   :else   (.listCollections db PersistentHashMap))
        it       (cond-> it
                   batch-size   (.batchSize (int batch-size))
                   comment      (.comment comment)
                   query        (.filter (c/map->bson query registry))
                   max-time-ms  (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
                   timeout-mode (.timeoutMode (di/timeout-mode->java timeout-mode)))]
    (cursor/->reducible it)))

(defn get-read-preference
  [{::keys [^MongoDatabase db]}]
  {:pre [db]}
  (-> db .getReadPreference di/read-preference->clj))

(defn get-read-concern
  [{::keys [^MongoDatabase db]}]
  {:pre [db]}
  (-> db .getReadConcern di/read-concern->clj))

(defn get-write-concern
  [{::keys [^MongoDatabase db]}]
  {:pre [db]}
  (-> db .getWriteConcern di/write-concern->clj))
