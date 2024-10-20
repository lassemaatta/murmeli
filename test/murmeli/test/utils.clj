(ns murmeli.test.utils
  (:require [clj-test-containers.core :as tc]
            [clojure.spec.test.alpha :as stest]
            [clojure.tools.logging :as log]
            [murmeli.core :as m])
  (:import [org.testcontainers.containers MongoDBContainer]))

(set! *warn-on-reflection* true)

(def *container (atom nil))

(stest/instrument `tc/init)

(def version-matrix ["mongo:6.0.18"
                     "mongo:7.0.14"
                     "mongo:8.0.1"])

(defn config
  [^String image]
  (tc/init {:container     (MongoDBContainer. image)
            :exposed-ports [27017]
            :wait-for      {:wait-strategy :port}}))

(defn container-fixture
  [test-fn]
  (doseq [image version-matrix]
    (when (compare-and-set! *container nil (delay (tc/start! (config image))))
      (log/infof "Starting container %s for tests" image))
    (test-fn)
    (let [c (deref *container)]
      (when (realized? c)
        (log/infof "Stopping container %s for tests" image)
        (tc/stop! (force c))))
    (reset! *container nil)))

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
  "Get the `db-spec` for connecting to the current test database"
  []
  (or *db-spec*
      (throw (ex-info "Mongo DB connection not running" {}))))
