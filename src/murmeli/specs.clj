(ns murmeli.specs
  "Optional `clojure.spec`'s for `murmeli.*`."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [murmeli.convert :as mc]
            [murmeli.core :as m]
            [murmeli.data-interop :as di])
  (:import [com.mongodb ClientSessionOptions
                        ConnectionString
                        MongoClientSettings
                        MongoNamespace
                        ReadConcern
                        ReadPreference
                        WriteConcern]
           [com.mongodb.client ClientSession MongoClient MongoDatabase]
           [com.mongodb.client.model IndexOptions UpdateOptions]
           [org.bson BsonValue]
           [org.bson.conversions Bson]))

(set! *warn-on-reflection* true)

(s/def ::non-blank-str (s/and string?
                              (complement str/blank?)))
(s/def ::integer (s/and integer?
                        #(< Integer/MIN_VALUE % Integer/MAX_VALUE)))

(s/def ::uri (s/and ::non-blank-str
                    (fn [^String uri]
                      (try
                        (ConnectionString. uri)
                        (catch Exception _
                          false)))))

(s/def ::id (s/or :str-id m/id?
                  :object-id m/object-id?))

(defn valid-db-name?
  [db-name]
  (try
    (MongoNamespace/checkDatabaseNameValidity db-name)
    true
    (catch IllegalArgumentException _
      false)))

(s/def ::database-name (s/and ::non-blank-str
                              valid-db-name?
                              ;; "Database names cannot be empty and must be less than 64 bytes."
                              (fn [s] (< (count s) 64))))

(defn valid-collection-name?
  [collection-name]
  (try
    (MongoNamespace/checkCollectionNameValidity collection-name)
    true
    (catch IllegalArgumentException _
      false)))

(def valid-collection? (every-pred valid-collection-name?
                                   ;; "Collection names should begin with an underscore or a letter character, and cannot"
                                   (fn [s] (re-matches #"[_a-zA-Z]+.*" s))
                                   ;; "contain the `$`"
                                   (fn [s] (not (str/includes? s "$")))
                                   ;; "begin with the `system.` prefix"
                                   (fn [s] (not (str/starts-with? s "system.")))))

(s/def ::collection (s/and ::key
                           (fn [[type value]]
                             (cond-> value
                               (= type :kw) name
                               true         valid-collection?))))

(defn db? [instance] (instance? MongoDatabase instance))
(s/def ::m/db db?)

(defn client? [instance] (instance? MongoClient instance))
(s/def ::m/client client?)

(defn session-options? [instance] (instance? ClientSessionOptions instance))
(s/def ::m/session-options session-options?)

(defn session? [instance] (instance? ClientSession instance))
(s/def ::m/session session?)

(s/def ::db-spec-disconnected (s/merge (s/keys :opt-un [::database-name])
                                       ::client-settings-options))

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
(s/def ::index-keys (s/map-of ::key ::index-type
                              ;; "A single collection can have no more than 64 indexes."
                              :min-count 1
                              :max-count 64))

(s/def ::background? boolean?)
(s/def ::name ::non-blank-str)
(s/def ::version ::integer)
(s/def ::unique? boolean?)
(s/def ::sparse? boolean?)
(s/def ::create-index-options (s/keys* :opt-un [::background?
                                                ::name
                                                ::version
                                                ::unique?
                                                ::sparse?]))

(s/fdef m/create-index!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :keys ::index-keys
               :options ::create-index-options))

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

(s/def ::documents (s/coll-of ::document
                              :min-count 1))

(s/fdef m/insert-many!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :docs ::documents))

(s/def ::upsert? boolean?)
(s/def ::update-options (s/keys* :opt-un [::upsert?]))

(s/fdef m/update-one!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query ::document
               :changes ::documents
               :options ::update-options))

(s/fdef m/update-many!
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query ::document
               :changes ::documents
               :options ::update-options))

(s/fdef m/count-collection
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :query (s/? ::document)))

(s/fdef m/estimated-count-collection
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection))

(s/def ::query ::document)
(s/def ::projection (s/coll-of ::key
                               :kind vector?))
(s/def ::sort (s/map-of ::key any?
                        ;; "You can sort on a maximum of 32 keys."
                        :min-count 1
                        :max-count 32))
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
                                            ::keywords?
                                            ::warn-on-multiple?
                                            ::throw-on-multiple?]))

(s/fdef m/find-one
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :options ::find-one-options))

(s/def ::find-by-id-options (s/keys* :opt-un [::projection
                                              ::keywords?]))

(s/fdef m/find-by-id
  :args (s/cat :db-spec ::db-spec-with-db
               :collection ::collection
               :id ::id
               :options ::find-by-id-options))

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

;; murmeli.data-interop

(defn read-preference?
  [object]
  (instance? ReadPreference object))

(s/fdef di/get-read-preference
  :args (s/cat :choice ::read-preference)
  :ret (s/nilable read-preference?))

(defn read-concern?
  [object]
  (instance? ReadConcern object))

(s/fdef di/get-read-concern
  :args (s/cat :choice ::read-concern)
  :ret (s/nilable read-concern?))

(defn write-concern?
  [object]
  (instance? WriteConcern object))

(s/fdef di/get-write-concern
  :args (s/cat :choice ::write-concern)
  :ret (s/nilable write-concern?))

(s/def ::retry-reads? boolean?)
(s/def ::retry-writes? boolean?)

(s/def ::enabled? boolean?)
(s/def ::invalid-hostname-allowed? boolean?)
(s/def ::ssl-settings (s/keys :opt-un [::enabled?
                                       ::invalid-hostname-allowed?]))

(s/def ::host ::non-blank-str)
(s/def ::port ::integer)
(s/def ::host-entry (s/keys :req-un [::host]
                            :opt-un [::port]))
(s/def ::hosts (s/coll-of ::host-entry
                          :kind vector?
                          :min-count 1))
(s/def ::cluster-settings (s/keys :opt-un [::hosts]))

(s/def ::username ::non-blank-str)
(s/def ::password ::non-blank-str)
(s/def ::auth-db ::database-name)
(s/def ::credentials (s/keys :req-un [::username
                                      ::password
                                      ::auth-db]))

(s/def ::client-settings-options (s/keys :opt-un [::uri
                                                  ::read-concern
                                                  ::write-concern
                                                  ::read-preference
                                                  ::retry-reads?
                                                  ::retry-writes?
                                                  ::credentials
                                                  ::cluster-settings
                                                  ::ssl-settings]))

(defn client-settings?
  [object]
  (instance? MongoClientSettings object))

(s/fdef di/make-client-settings
  :args (s/cat :options ::client-settings-options)
  :ret client-settings?)

(defn session-opts-valid?
  [{:keys [causally-consistent? snapshot?]}]
  ;; May not be both causally consistent and snapshot
  (not (and causally-consistent? snapshot?)))

(s/def ::client-session-options (s/and (s/keys :opt-un [::read-preference
                                                        ::read-concern
                                                        ::write-concern
                                                        ::causally-consistent?
                                                        ::snapshot?])
                                       session-opts-valid?))

(s/fdef di/make-client-session-options
  :args (s/cat :options ::client-session-options)
  :ret session-options?)

(s/def ::make-index-options (s/keys :opt-un [::background?
                                             ::name
                                             ::version
                                             ::unique?
                                             ::sparse?]))

(defn index-options?
  [object]
  (instance? IndexOptions object))

(s/fdef di/make-index-options
  :args (s/cat :options ::make-index-options)
  :ret index-options?)

(s/fdef di/make-index-bson
  :args (s/cat :index-keys ::index-keys)
  :ret bson?)

(s/def ::make-update-options (s/keys :opt-un [::upsert?]))

(defn update-options?
  [object]
  (instance? UpdateOptions object))

(s/fdef di/make-update-options
  :args (s/cat :options ::make-update-options)
  :ret (s/nilable update-options?))
