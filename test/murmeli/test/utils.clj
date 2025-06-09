(ns murmeli.test.utils
  (:require [clj-test-containers.core :as tc]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [testing]]
            [clojure.tools.logging :as log]
            [murmeli.core :as m])
  (:import [org.testcontainers.containers MongoDBContainer]))

(set! *warn-on-reflection* true)

;; Map of image name to `:container` & `:clients`
(defonce *containers (atom {}))

(def ^:dynamic *container* "Currently active container" nil)

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

(defn get-mongo-port
  [container]
  (get-in container [:mapped-ports 27017]))

(defn get-mongo-uri
  [container]
  (let [port (get-mongo-port container)]
    (format "mongodb://localhost:%d" port)))

(def ^:dynamic *conn* nil)

(defn db-spec
  [container]
  {:uri       (get-mongo-uri container)
   :keywords? true})

(defn connect!
  [container]
  (-> (db-spec container)
      m/connect-client!))

(defn get-conn
  "Get the `conn` for connecting to the current test database"
  []
  (or *conn*
      (throw (ex-info "Mongo DB connection not running" {}))))

(defn register-image-client
  [containers image]
  (if (get containers image)
    ;; Someone has already initialized the container, increase number of clients
    (update-in containers [image :clients] inc)
    ;; We're the first, create delay for initializing the container
    (assoc containers image {:container (delay
                                          (log/infof "Created container %s for tests" image)
                                          (tc/start! (config image)))
                             :clients   1})))

(defn unregister-image
  [containers image]
  (update-in containers [image :clients] dec))

(defmacro with-matrix
  "Execute `body` for each container in `version-matrix`.

  Note that we effectively run each test var N times in a row. Although we avoid
  recreating the containers between each test var, we do create a new Mongo connection every time
  as sharing the same connection between tests might not be safe. The good news is that creating
  a new connection is relatively fast.

  Also, use a macro instead of a fixture; `eftest` gets confused if a fixture calls `test-fn`
  multiple times."
  [& body]
  `(doseq [image# version-matrix]
     (testing (str "With image " image#)
       (log/infof "Selecting image: %s" image#)
       (let [containers# (swap! *containers register-image-client image#)
             container#  (get-in containers# [image# :container])
             db-name#    (str (gensym "test-db"))]
         (binding [*container* (force container#)
                   *conn*      (-> (connect! (force container#))
                                   (m/with-db db-name#)
                                   (m/with-default-registry))]
           (try
             (do ~@body)
             (finally
               (m/disconnect! *conn*))))
         (swap! *containers unregister-image image#)
         (log/infof "Finished with %s" image#)))))

(defn remove-unused-container
  [containers image]
  (let [{:keys [container clients]} (get containers image)]
    (if (and container
             (realized? container)
             (zero? clients))
      (dissoc containers image)
      containers)))

(defn container-cleanup-fixture
  "Check if containers can be cleaned once we finish testing a namespace.
  Not sure if this is strictly necessary, but it feels like a good idea to cleanup afterwards."
  [test-fn]
  (try
    (test-fn)
    (finally
      (when (not retain-containers?)
        (doseq [image version-matrix]
          ;; Note: We might be executing tests in multiple threads
          (let [[old-cs new-cs]     (swap-vals! *containers remove-unused-container image)
                {:keys [container]} (get old-cs image)]
            ;; _We_ just removed a container -> we must/can clean it up
            (when (and container
                       (nil? (get-in new-cs [image :container])))
              (log/infof "Stopping container %s for tests" image)
              (tc/stop! (force container)))))))))
