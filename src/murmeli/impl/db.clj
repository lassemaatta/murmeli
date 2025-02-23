(ns murmeli.impl.db
  "Database implementation.

  See [MongoDatabase](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoDatabase.html)."
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.client :as client]
            [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession MongoCollection MongoDatabase]
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

(defn with-registry
  [{::keys [^MongoDatabase db] :as conn}
   ^CodecRegistry registry]
  {:pre [conn db registry]}
  (assoc conn ::db (.withCodecRegistry db registry)))

(defn with-default-registry
  [conn]
  (with-registry conn (c/registry {:keywords?        true
                                   :allow-qualified? false})))

(defn registry
  ^CodecRegistry [{::keys [^MongoDatabase db]}]
  (.getCodecRegistry db))

(defn drop-db!
  [{::client/keys [^ClientSession session]
    :as           conn}
   database-name]
  {:pre [conn database-name]}
  (let [db (client/get-database conn database-name)]
    (if session
      (.drop db session)
      (.drop db))))

;;; Collection

(defn create-collection!
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]}
   collection
   & {:as options}]
  {:pre [db collection]}
  (let [options (when (seq options)
                  (di/make-create-collection-options options))]
    (cond
      (and session options) (.createCollection db session (name collection) options)
      session               (.createCollection db session (name collection))
      options               (.createCollection db (name collection) options)
      :else                 (.createCollection db (name collection)))))

(defn get-collection
  (^MongoCollection
   [conn collection]
   (get-collection conn collection nil))
  (^MongoCollection
   [{::keys [^MongoDatabase db] :as conn} collection opts]
   {:pre [db collection]}
   (let [opts          (select-keys opts c/registry-options-keys)
         registry-opts (when (seq opts)
                         (merge (select-keys conn c/registry-options-keys)
                                opts))]
     (cond-> (.getCollection db (name collection) PersistentHashMap)
       (seq registry-opts) (.withCodecRegistry (c/registry registry-opts))))))

(defn list-collection-names-reducible
  [{::keys        [^MongoDatabase db]
    ::client/keys [^ClientSession session]
    :as           conn}
   & {:keys [authorized-collections?
             batch-size
             ^String comment
             max-time-ms
             query]}]
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
    (cursor/->reducible it)))

(defn list-collection-names
  [conn & {:keys [keywords?]
           :or   {keywords? true}
           :as   options}]
  {:pre [conn]}
  (cond->> (list-collection-names-reducible conn options)
    keywords? (eduction (map keyword))
    true      (into #{})))
