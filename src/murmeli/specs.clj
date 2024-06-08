(ns murmeli.specs
  "Optional `clojure.spec`'s for `murmeli.core`."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [murmeli.core :as m])
  (:import [com.mongodb ClientSessionOptions]
           [com.mongodb.client ClientSession MongoClient MongoDatabase]))

(s/def ::non-blank-str (s/and string?
                              (complement str/blank?)))
(s/def ::uri ::non-blank-str)

(defn db? [instance] (instance? MongoDatabase instance))
(s/def ::m/db db?)

(defn client? [instance] (instance? MongoClient instance))
(s/def ::m/client client?)

(defn session-options? [instance] (instance? ClientSessionOptions instance))
(s/def ::m/session-options session-options?)

(defn session? [instance] (instance? ClientSession instance))
(s/def ::m/session session?)

(s/def ::db-spec-disconnected (s/keys :req-un [::uri]))

(s/def ::db-spec-with-client (s/merge ::db-spec-disconnected
                                      (s/keys :req [::m/client])))

(s/def ::db-spec-with-db (s/merge ::db-spec-with-client
                                  (s/keys :req [::m/db]
                                          :opt [::m/session-options
                                                ::m/session])))

(s/fdef m/connect-client!
  :args (s/cat :db-spec ::db-spec-disconnected)
  :ret ::db-spec-with-client)

(s/fdef m/connect-db!
  :args (s/cat :db-spec ::db-spec-with-client)
  :ret ::db-spec-with-db)

(s/fdef m/disconnect
  :args (s/cat :db-spec ::db-spec-with-db)
  :ret ::db-spec-disconnected)

(s/def ::read-preference #{:nearest
                           :primary
                           :secondary
                           :primary-preferred
                           :secondary-preferred})

(s/def ::read-concern #{:available
                        :local
                        :linearizable
                        :snapshot
                        :majority
                        :default})

(s/def ::write-concern #{:w1
                         :w2
                         :w3
                         :majority
                         :journaled
                         :acknowledged
                         :unacknowledged})

(s/def ::causally-consistent? boolean?)
(s/def ::snapshot? boolean?)

(s/def ::session-options (s/keys :opt-un [::read-preference
                                          ::read-concern
                                          ::write-concern
                                          ::causally-consistent?
                                          ::snapshot?]))

(s/fdef m/with-client-session-options
  :args (s/cat :db-spec ::db-spec-with-db
               :options ::session-options)
  :ret ::db-spec-with-db)

(s/fdef m/start-session!
  :args (s/cat :db-spec ::db-spec-with-db
               :session-opts ::m/session-options))

(s/def ::with-session-bindings (s/tuple simple-symbol?
                                        some?))

(s/fdef m/with-session
  :args (s/cat :bindings ::with-session-bindings
               :body (s/* any?)))

(s/def ::collection (s/or :kw simple-keyword?
                          :str ::non-blank-str))

(s/def ::index-type #{"2d"
                      "2dsphere"
                      "text"
                      1
                      -1})
(s/def ::index-keys (s/map-of simple-keyword? ::index-type))

(s/def ::background ::non-blank-str)
(s/def ::name ::non-blank-str)
(s/def ::version ::non-blank-str)
(s/def ::unique? boolean?)
(s/def ::sparse? boolean?)
(s/def ::create-index-options (s/keys :opt-un [::background
                                               ::name
                                               ::version
                                               ::unique?
                                               ::sparse?]))

(s/fdef m/create-index!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :keys ::index-keys
               :options (s/? ::create-index-options)))

(s/fdef m/list-indexes
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection))

(s/fdef m/drop-all-indexes!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection))

(s/fdef m/drop-index!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :keys ::index-keys))

(s/fdef m/drop-index-by-name!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :index-name ::name))

(s/def ::document map?)

(s/fdef m/insert-one!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :doc ::document))

(s/fdef m/count-collection
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query (s/? ::document)))

(s/fdef m/estimated-count-collection
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection))

(s/def ::limit int?)
(s/def ::skip int?)
(s/def ::batch-size int?)
(s/def ::keywords? boolean?)

(s/def ::find-all-options (s/keys :opt-un [::limit
                                           ::skip
                                           ::batch-size
                                           ::keywords?]))

(s/fdef m/find-all
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query (s/? ::document)
               :options (s/? ::find-all-options)))

(s/def ::warn-on-multiple? boolean?)
(s/def ::throw-on-multiple? boolean?)
(s/def ::find-one-options (s/keys :opt-un [::warn-on-multiple?
                                           ::throw-on-multiple?]))

(s/fdef m/find-one
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query (s/? ::document)
               :options (s/? ::find-one-options)))
