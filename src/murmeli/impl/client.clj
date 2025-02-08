(ns murmeli.impl.client
  (:require [murmeli.impl.data-interop :as di])
  (:import [com.mongodb.client MongoClient MongoClients]))

(set! *warn-on-reflection* true)

(defn connect-client!
  [db-spec]
  {::client (MongoClients/create (di/make-client-settings db-spec))})

(defn connected?
  [{::keys [client]}]
  (some? client))

(defn disconnect!
  [{::keys [^MongoClient client]}]
  (when client
    (.close client)))
