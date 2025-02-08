(ns murmeli.impl.db
  "Database implementation"
  {:no-doc true}
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.client :as client]
            [murmeli.impl.session :as session])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession MongoClient MongoDatabase]))

(set! *warn-on-reflection* true)

(defn get-database
  "Find a database by name."
  ^MongoDatabase
  [{::client/keys [^MongoClient client]}
   database-name]
  {:pre [client database-name]}
  (.getDatabase client database-name))

(defn with-db
  [{::keys [^MongoDatabase db] :as conn}
   database-name]
  {:pre [database-name]}
  (when-not (client/connected? conn)
    (throw (ex-info "Cannot retrieve database without a connection" {:database-name database-name})))
  (if-not (and db (= database-name (.getName db)))
    (do
      (log/debugf "Loading database %s" database-name)
      (assoc conn ::db (get-database conn database-name)))
    conn))

(defn list-dbs
  [{::client/keys  [^MongoClient client]
    ::session/keys [^ClientSession session]}]
  (log/debugf "list databases")
  (let [it (cond
             session (.listDatabases client session PersistentHashMap)
             :else   (.listDatabases client PersistentHashMap))]
    (into [] it)))

(defn drop-db!
  [{::session/keys [^ClientSession session]
    :as            conn}
   database-name]
  {:pre [conn database-name]}
  (let [db (get-database conn database-name)]
    (if session
      (.drop db session)
      (.drop db))))
