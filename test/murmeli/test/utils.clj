(ns murmeli.test.utils
  (:require [clj-test-containers.core :as tc]
            [clojure.spec.test.alpha :as stest]
            [clojure.tools.logging :as log]
            [murmeli.core :as m])
  (:import [org.testcontainers.containers MongoDBContainer]))

(set! *warn-on-reflection* true)

(defonce *containers (atom {}))
(def ^:dynamic *container* nil)

(def retain-containers?
  "Cache containers between tests when running in a REPL"
  (= "true" (System/getProperty "murmeli.repl")))

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
    (log/infof "Selecting image: %s" image)
    (let [containers (swap! *containers (fn [containers]
                                          (if (get containers image)
                                            containers
                                            (assoc containers image (delay (tc/start! (config image)))))))
          container  (get containers image)]
      (when (not (realized? container))
        (log/infof "Created container %s for tests" image))
      (binding [*container* container]
        (test-fn))
      (let [c (get (deref *containers) image)]
        (when (and (realized? c) (not retain-containers?))
          (log/infof "Stopping container %s for tests" image)
          (tc/stop! (force c))))
      (log/infof "Finished with %s" image))))

(defn get-mongo-port
  []
  (let [container *container*]
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
                           :database-name "test-db"
                           :keywords?     true}
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
