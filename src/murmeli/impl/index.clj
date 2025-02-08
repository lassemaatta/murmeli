(ns murmeli.impl.index
  "Index implementation"
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.collection :as collection]
            [murmeli.impl.convert :as c]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession ListIndexesIterable]
           [com.mongodb.client.model IndexOptions]
           [java.util.concurrent TimeUnit]))

(set! *warn-on-reflection* true)

(defn create-index!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   index-keys
   & {:keys [partial-filter-expression
             storage-engine
             weights]
      :as   options}]
  {:pre [conn collection (map? index-keys)]}
  (log/debugf "create index; %s %s %s" collection index-keys options)
  (let [coll             (collection/get-collection conn collection)
        registry         (.getCodecRegistry coll)
        index-keys       (di/make-index-bson index-keys)
        ^IndexOptions io (cond-> options
                           partial-filter-expression (update :partial-filter-expression c/map->bson registry)
                           storage-engine            (update :storage-engine c/map->bson registry)
                           weights                   (update :weights c/map->bson registry)
                           (seq options)             (di/make-index-options))]
    (cond
      (and io session) (.createIndex coll session index-keys io)
      session          (.createIndex coll session index-keys)
      io               (.createIndex coll index-keys io)
      :else            (.createIndex coll index-keys))))

(defn list-indexes
  [{::session/keys [^ClientSession session] :as conn}
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
  (let [coll                    (collection/get-collection conn collection {:keywords? keywords?})
        ^ListIndexesIterable it (if session
                                  (.listIndexes coll session PersistentHashMap)
                                  (.listIndexes coll PersistentHashMap))]
    (when batch-size (.batchSize it (int batch-size)))
    (when max-time-ms (.maxTime it (long max-time-ms) TimeUnit/MILLISECONDS))
    (into [] it)))

(defn drop-all-indexes!
  [{::session/keys [^ClientSession session] :as conn}
   collection]
  {:pre [conn collection]}
  (let [coll (collection/get-collection conn collection)]
    (if session
      (.dropIndexes coll session)
      (.dropIndexes coll))))

(defn drop-index!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   index-keys]
  {:pre [conn collection (seq index-keys)]}
  (let [coll       (collection/get-collection conn collection)
        index-keys (di/make-index-bson index-keys)]
    (cond
      session (.dropIndex coll session index-keys)
      :else   (.dropIndex coll index-keys))))

(defn drop-index-by-name!
  [{::session/keys [^ClientSession session] :as conn}
   collection
   ^String index-name]
  {:pre [conn collection index-name]}
  (let [coll (collection/get-collection conn collection)]
    (cond
      session (.dropIndex coll session index-name)
      :else   (.dropIndex coll index-name))))
