(ns murmeli.impl.client
  "Client implementation.

  See [MongoClient](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoClient.html)
  and [MongoCluster](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoClient.html)."
  {:no-doc true}
  (:require [murmeli.impl.cursor :as cursor]
            [murmeli.impl.data-interop :as di])
  (:import [clojure.lang PersistentHashMap]
           [com.mongodb.client ClientSession
                               MongoClient
                               MongoClients
                               MongoDatabase]))

(set! *warn-on-reflection* true)

;;; Client

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

;;; Database

(defn get-database
  "Find a database by name."
  ^MongoDatabase
  [{::keys [^MongoClient client]}
   database-name]
  {:pre [client database-name]}
  (.getDatabase client database-name))

(defn list-dbs
  [{::keys [^MongoClient client ^ClientSession session]}]
  (let [it (cond
             session (.listDatabases client session PersistentHashMap)
             :else   (.listDatabases client PersistentHashMap))]
    (into [] (cursor/->reducible it))))

;;; Session

(defn with-client-session-options
  [conn & {:as options}]
  (assoc conn ::session-options (di/make-client-session-options (or options {}))))

(defn get-session-options
  [conn]
  (or (::session-options conn)
      (throw (ex-info "No session options specified, call `with-client-session-options`"
                      {}))))

(defn start-session!
  ^ClientSession
  [{::keys [^MongoClient client]}
   session-opts]
  {:pre [client session-opts]}
  (.startSession client session-opts))

(defn with-session
  [conn session]
  (assoc conn ::session session))

(defn with-session*
  [[sym conn :as bindings] body]
  {:pre [(vector? bindings)
         (= 2 (count bindings))
         (simple-symbol? sym)]}
  `(let [conn#                   ~conn
         session-opts#           (get-session-options conn#)
         ^ClientSession session# (start-session! conn# session-opts#)
         ~sym                    (with-session conn# session#)]
     (try
       (.startTransaction session#)
       (let [result# (do ~@body)]
         (.commitTransaction session#)
         result#)
       (catch Exception e#
         (.abortTransaction session#)
         (throw e#)))))
