(ns murmeli.impl.collection
  "Collection implementation"
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.convert :as c]
            [murmeli.impl.db :as db]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession MongoCollection MongoDatabase]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(defn create-collection!
  [{::db/keys      [^MongoDatabase db]
    ::session/keys [^ClientSession session]}
   collection]
  {:pre [db collection]}
  ;; TODO: Add support for `CreateCollectionOptions`
  (cond
    session (.createCollection db session (name collection))
    :else   (.createCollection db (name collection))))

(defn get-collection
  (^MongoCollection
   [conn collection]
   (get-collection conn collection {}))
  (^MongoCollection
   [{::db/keys [^MongoDatabase db] :as conn} collection opts]
   {:pre [db collection opts]}
   (let [registry-opts (merge (select-keys conn [:keywords?])
                              opts)]
     (-> db
         (.getCollection (name collection) PersistentHashMap)
         (.withCodecRegistry (c/registry registry-opts))))))

(defn list-collection-names
  [{::db/keys      [^MongoDatabase db]
    ::session/keys [^ClientSession session]
    :as            conn}
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
  [{::session/keys [^ClientSession session] :as conn}
   collection]
  (let [coll (get-collection conn collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))
