(ns murmeli.impl.collection
  "Collection implementation"
  {:no-doc true}
  (:require [murmeli.impl.convert :as c]
            [murmeli.impl.cursor :as cursor]
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

(defn list-collection-names
  [{::db/keys      [^MongoDatabase db]
    ::session/keys [^ClientSession session]
    :as            conn}
   & {:keys [batch-size
             max-time-ms
             keywords?]
      :or   {keywords? true}}]
  {:pre [conn]}
  (let [it (cond
             session (.listCollectionNames db session)
             :else   (.listCollectionNames db))
        ;; TODO: add `filter` parameter
        ;; TODO: add `authorizedCollections` parameter
        it (cond-> it
             batch-size  (.batchSize (int batch-size))
             max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))
        r  (cursor/->reducible it)]
    ;; TODO separate -reducible variant?
    (if keywords?
      (into #{} (map keyword) r)
      (into #{} r))))

(defn drop-collection!
  [{::session/keys [^ClientSession session] :as conn}
   collection]
  (let [coll (get-collection conn collection)]
    (cond
      session (.drop coll session)
      :else   (.drop coll))))
