(ns murmeli.impl.session
  (:require [murmeli.impl.client :as client]
            [murmeli.impl.data-interop :as di])
  (:import [com.mongodb.client ClientSession MongoClient]))

(set! *warn-on-reflection* true)

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
  [{::client/keys [^MongoClient client]}
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
