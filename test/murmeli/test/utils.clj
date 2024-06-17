(ns murmeli.test.utils
  (:require [clj-test-containers.core :as tc]
            [clojure.spec.test.alpha :as stest]
            [clojure.tools.logging :as log]
            [murmeli.core :as m])
  (:import [org.testcontainers.containers MongoDBContainer]))

(set! *warn-on-reflection* true)

(def *container (atom nil))

(stest/instrument `tc/init)

(def config (tc/init {:container     (MongoDBContainer. "mongo:7.0.11")
                      :exposed-ports [27017]
                      :wait-for      {:wait-strategy :port}}))

(defn container-fixture
  [test-fn]
  (when (compare-and-set! *container nil (delay (tc/start! config)))
    (log/info "Starting container for tests"))
  (test-fn))

(defn get-mongo-port
  []
  (let [container @*container]
    (when-not container
      (throw (ex-info "Container not running" {})))
    (get-in @container [:mapped-ports 27017])))

(defn get-mongo-uri
  []
  (let [port (get-mongo-port)]
    (format "mongodb://localhost:%d" port)))

(def ^:dynamic *db-spec* nil)

(defn db-fixture
  [test-fn]
  (binding [*db-spec* (-> {:uri           (get-mongo-uri)
                           :database-name "test-db"}
                          m/connect-client!
                          m/with-db)]
    (try
      (test-fn)
      (finally
        (m/disconnect! *db-spec*)))))

(defn get-db-spec
  []
  (or *db-spec*
      (throw (ex-info "Mongo DB connection not running" {}))))
