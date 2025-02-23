(ns murmeli.impl.collection
  "Collection implementation"
  {:no-doc true}
  (:require [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.db :as db]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession MongoCollection MongoDatabase]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(defn create-collection!
  [{::db/keys      [^MongoDatabase db]
    ::session/keys [^ClientSession session]}
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
   [{::db/keys [^MongoDatabase db] :as conn} collection opts]
   {:pre [db collection]}
   (let [opts          (select-keys opts c/registry-options-keys)
         registry-opts (when (seq opts)
                         (merge (select-keys conn c/registry-options-keys)
                                opts))]
     (cond-> (.getCollection db (name collection) PersistentHashMap)
       (seq registry-opts) (.withCodecRegistry (c/registry registry-opts))))))

(defn list-collection-names-reducible
  [{::db/keys      [^MongoDatabase db]
    ::session/keys [^ClientSession session]
    :as            conn}
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

(defn drop-collection!
  [{::session/keys [^ClientSession session] :as conn}
   collection]
  (let [coll (get-collection conn collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))
