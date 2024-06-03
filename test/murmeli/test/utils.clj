(ns murmeli.test.utils
  (:require [clj-test-containers.core :as tc]
            [clojure.tools.logging :as log]
            [murmeli.core :as m]))

(set! *warn-on-reflection* true)

(def ^:dynamic *container* nil)

(def config (tc/create {:image-name    "mongo:7.0.11"
                        :exposed-ports [27017]}))

(defn container-fixture
  [test-fn]
  (log/info "Starting container for tests")
  (binding [*container* (tc/start! config)]
    (try
      (test-fn)
      (finally
        (log/info "Stopping container for tests")
        (tc/stop! *container*)))))

(defn get-mongo-port
  []
  (when-not *container*
    (throw (ex-info "Container not running" {})))
  (get-in *container* [:mapped-ports 27017]))

(defn get-mongo-uri
  []
  (let [port (get-mongo-port)]
    (format "mongodb://localhost:%d" port)))

(def ^:dynamic *conn* nil)

(defn db-fixture
  [test-fn]
  (binding [*conn* (-> {:uri      (get-mongo-uri)
                        :database "test-db"}
                       m/connect-client!
                       m/connect-db!)]
    (try
      (test-fn)
      (finally
        (m/disconnect! *conn*)))))

(defn get-conn
  []
  (or *conn*
      (throw (ex-info "Mongo DB connection not running" {}))))
