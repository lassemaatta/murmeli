(ns murmeli.specs
  "Optional `clojure.spec`'s for `murmeli.*`."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [murmeli.convert :as mc]
            [murmeli.core :as m])
  (:import [com.mongodb ClientSessionOptions MongoNamespace]
           [com.mongodb.client ClientSession MongoClient MongoDatabase]
           [org.bson BsonValue]
           [org.bson.conversions Bson]))

(set! *warn-on-reflection* true)

(s/def ::non-blank-str (s/and string?
                              (complement str/blank?)))
(s/def ::uri ::non-blank-str)

(defn valid-db-name?
  [db-name]
  (try
    (MongoNamespace/checkDatabaseNameValidity db-name)
    true
    (catch IllegalArgumentException e
      false)))

(s/def ::database-name (s/and ::non-blank-str
                              valid-db-name?))

(defn valid-collection-name?
  [collection-name]
  (try
    (MongoNamespace/checkCollectionNameValidity collection-name)
    true
    (catch IllegalArgumentException e
      false)))

(s/def ::collection (s/and (s/nonconforming ::key)
                           (comp valid-collection-name? name)))

(defn db? [instance] (instance? MongoDatabase instance))
(s/def ::m/db db?)

(defn client? [instance] (instance? MongoClient instance))
(s/def ::m/client client?)

(defn session-options? [instance] (instance? ClientSessionOptions instance))
(s/def ::m/session-options session-options?)

(defn session? [instance] (instance? ClientSession instance))
(s/def ::m/session session?)

(s/def ::db-spec-disconnected (s/keys :req-un [::uri]
                                      :opt-un [::database-name]))

(s/def ::db-spec-with-client (s/merge ::db-spec-disconnected
                                      (s/keys :req [::m/client]
                                              :opt [::m/session-options
                                                    ::m/session])))

(s/def ::db-spec-with-db (s/merge ::db-spec-with-client
                                  (s/keys :req [::m/db])))

(s/fdef m/connect-client!
  :args (s/cat :db-spec ::db-spec-disconnected)
  :ret ::db-spec-with-client)

(s/fdef m/with-db
  :args (s/cat :db-spec ::db-spec-with-client
               :database-name (s/? ::database-name))
  :ret ::db-spec-with-db)

(s/fdef m/disconnect
  :args (s/cat :db-spec ::db-spec-with-client)
  :ret ::db-spec-disconnected)

(s/fdef m/list-dbs
  :args (s/cat :db-spec ::db-spec-with-client))

(s/fdef m/drop-db!
  :args (s/cat :db-spec ::db-spec-with-client
               :database-name ::database-name))

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

(s/def ::session-options (s/keys* :opt-un [::read-preference
                                           ::read-concern
                                           ::write-concern
                                           ::causally-consistent?
                                           ::snapshot?]))

(s/fdef m/with-client-session-options
  :args (s/cat :db-spec ::db-spec-with-client
               :options ::session-options)
  :ret ::db-spec-with-client)

(s/fdef m/start-session!
  :args (s/cat :db-spec ::db-spec-with-client
               :session-opts ::m/session-options))

(s/def ::with-session-bindings (s/tuple simple-symbol?
                                        some?))

(s/fdef m/with-session
  :args (s/cat :bindings ::with-session-bindings
               :body (s/* any?)))


(s/def ::key (s/or :kw simple-keyword? :str ::non-blank-str))

(s/def ::index-type #{"2d"
                      "2dsphere"
                      "text"
                      1
                      -1})
(s/def ::index-keys (s/map-of ::key ::index-type))

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

(s/def ::document (s/map-of ::key any?))

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

(s/def ::query ::document)
(s/def ::projection (s/coll-of ::key
                               :kind vector))
(s/def ::sort ::document)
(s/def ::limit int?)
(s/def ::skip int?)
(s/def ::batch-size int?)
(s/def ::keywords? boolean?)
(s/def ::xform fn?)

(s/def ::find-all-options (s/keys* :opt-un [::query
                                            ::projection
                                            ::sort
                                            ::xform
                                            ::limit
                                            ::skip
                                            ::batch-size
                                            ::keywords?]))

(s/fdef m/find-all
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :options ::find-all-options))

(s/def ::warn-on-multiple? boolean?)
(s/def ::throw-on-multiple? boolean?)
(s/def ::find-one-options (s/keys* :opt-un [::query
                                            ::projection
                                            ::sort
                                            ::xform
                                            ::keywords?
                                            ::warn-on-multiple?
                                            ::throw-on-multiple?]))

(s/fdef m/find-one
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :options ::find-one-options))

;; murmeli.convert

(defn bson-value?
  [object]
  (instance? BsonValue object))

(s/fdef mc/to-bson
  :args (s/cat :object any?)
  :ret bson-value?)

(defn bson?
  [object]
  (instance? Bson object))

(s/fdef mc/map->bson
  :args (s/cat :m map?)
  :ret bson?)

(s/def ::from-bson-options (s/keys :opt-un [::keywords?]))

(s/fdef mc/from-bson
  :args (s/cat :options (s/? ::from-bson-options)
               :bson bson-value?))
