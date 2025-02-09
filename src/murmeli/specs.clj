(ns murmeli.specs
  "Optional `clojure.spec`'s for `murmeli.*`."
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as str]
            [murmeli.core :as m]
            [murmeli.impl.client :as client]
            [murmeli.impl.convert :as mc]
            [murmeli.impl.data-interop :as di]
            [murmeli.impl.db :as db]
            [murmeli.impl.session :as session])
  (:import [com.mongodb ClientSessionOptions
                        ConnectionString
                        MongoClientSettings
                        MongoNamespace
                        ReadConcern
                        ReadPreference
                        WriteConcern]
           [com.mongodb.client ClientSession MongoClient MongoDatabase]
           [com.mongodb.client.model Collation
                                     CountOptions
                                     FindOneAndDeleteOptions
                                     FindOneAndReplaceOptions
                                     FindOneAndUpdateOptions
                                     IndexOptions
                                     ReplaceOptions
                                     UpdateOptions]
           [java.util.regex Pattern]
           [org.bson BsonValue]
           [org.bson.codecs.configuration CodecRegistry]
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

(s/def ::object-id m/object-id?)

(s/def ::id (s/or :str-id m/id?
                  :object-id ::object-id
                  :any-str string?))

(s/def ::registry (fn [v] (instance? CodecRegistry v)))

(defn bson?
  [object]
  (instance? Bson object))

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
(s/def ::db/db db?)

(defn client? [instance] (instance? MongoClient instance))
(s/def ::client/client client?)

(defn session-options? [instance] (instance? ClientSessionOptions instance))
(s/def ::session/session-options session-options?)

(defn session? [instance] (instance? ClientSession instance))
(s/def ::session/session session?)

(s/def ::db-spec (s/merge (s/keys :opt-un [::database-name])
                          ::client-settings-options))

(s/def ::conn (s/keys :req [::client/client]
                      :opt [::session/session-options
                            ::session/session]))

(s/def ::conn-with-db (s/merge ::conn
                               (s/keys :req [::db/db])))

(s/fdef m/connect-client!
  :args (s/cat :db-spec ::db-spec)
  :ret ::conn)

(s/fdef m/with-db
  :args (s/cat :conn ::conn
               :database-name (s/? ::database-name))
  :ret ::conn-with-db)

(s/fdef m/disconnect
  :args (s/cat :conn ::conn)
  :ret nil?)

(s/fdef m/list-dbs
  :args (s/cat :conn ::conn))

(s/fdef m/drop-db!
  :args (s/cat :conn ::conn
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
  :args (s/cat :conn ::conn
               :options ::session-options)
  :ret ::conn)

(s/fdef m/start-session!
  :args (s/cat :conn ::conn
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

(s/def ::document (s/map-of ::key ::convertable))

(s/def ::bson bson?)

(s/def ::background? boolean?)
(s/def ::bits ::integer)
(s/def ::default-language ::non-blank-str)
(s/def ::expire-after-seconds int?)
(s/def ::index-name ::non-blank-str)
(s/def ::partial-filter-expression ::document)
(s/def :bson/partial-filter-expression ::bson)
(s/def ::version ::integer)
(s/def ::unique? boolean?)
(s/def ::sparse? boolean?)
(s/def ::create-index-options (s/keys* :opt-un [::background?
                                                ::bits
                                                ::default-language
                                                ::expire-after-seconds
                                                ::index-name
                                                ::partial-filter-expression
                                                ::sparse?
                                                ::unique?
                                                ::version]))

(s/fdef m/create-index!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :keys ::index-keys
               :options ::create-index-options))

(s/def ::list-indexes-options (s/keys* :opt-un [::batch-size
                                                ::max-time-ms]))

(s/fdef m/list-indexes
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :options ::list-indexes-options))

(s/fdef m/drop-all-indexes!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection))

(s/fdef m/drop-index!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :keys ::index-keys))

(s/fdef m/drop-index-by-name!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :index-name ::index-name))

(s/fdef m/insert-one!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :doc ::document))

(s/def ::documents (s/coll-of ::document
                              :min-count 1))

(s/fdef m/insert-many!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :docs ::documents))

(s/def ::upsert? boolean?)
(s/def ::update-options (s/keys* :opt-un [::upsert?]))

(s/fdef m/update-one!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :query ::document
               :changes ::document
               :options ::update-options))

(s/fdef m/update-many!
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :query ::document
               :changes ::document
               :options ::update-options))

(s/def ::count-options (s/keys* :opt-un [::query
                                         ::limit
                                         ::skip
                                         ::max-time-ms]))

(s/fdef m/count-collection
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :count-options ::count-options))

(s/fdef m/estimated-count-collection
  :args (s/cat :conn ::conn-with-db
               :collection ::collection))

(s/def ::query ::document)
(s/def ::projection-val #{-1 1 true false})
(s/def ::projection-map (s/map-of ::key ::projection-val))
(s/def ::projection-list (s/coll-of ::key))
(s/def ::projection (s/or :list ::projection-list
                          :map ::projection-map))
(s/def ::sort (s/map-of ::key any?
                        ;; "You can sort on a maximum of 32 keys."
                        :min-count 1
                        :max-count 32))
(s/def ::limit ::integer)
(s/def ::skip ::integer)
(s/def ::batch-size int?)
(s/def ::max-time-ms int?)
(s/def ::keywords? boolean?)
(s/def ::xform fn?)

(s/def ::find-all-options (s/keys* :opt-un [::query
                                            ::projection
                                            ::sort
                                            ::xform
                                            ::limit
                                            ::skip
                                            ::batch-size
                                            ::max-time-ms
                                            ::keywords?]))

(s/fdef m/find-all
  :args (s/cat :conn ::conn-with-db
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
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :options ::find-one-options))

(s/def ::find-by-id-options (s/keys* :opt-un [::projection
                                              ::keywords?]))

(s/fdef m/find-by-id
  :args (s/cat :conn ::conn-with-db
               :collection ::collection
               :id ::id
               :options ::find-by-id-options))

;; murmeli.convert

(defn bson-value?
  [object]
  (instance? BsonValue object))

(defn regex?
  [re]
  (instance? Pattern re))

(s/def ::regex regex?)

(s/def ::convertable (s/or :keyword simple-keyword?
                           :string string?
                           :int int?
                           :boolean boolean?
                           :inst inst?
                           :nil nil?
                           :object-id ::object-id
                           :pattern ::regex
                           :set (s/coll-of ::convertable :into #{})
                           :vec (s/coll-of ::convertable :into [])
                           :map ::document))

(s/fdef mc/to-bson
  :args (s/cat :object ::convertable)
  :ret bson-value?)

(s/fdef mc/map->bson
  :args (s/cat :m map? :registry ::registry)
  :ret ::bson)

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

(s/def ::default-timeout-ms nat-int?)

(s/def ::client-session-options (s/and (s/keys :opt-un [::causally-consistent?
                                                        ::default-timeout-ms
                                                        ::read-concern
                                                        ::read-preference
                                                        ::snapshot?
                                                        ::write-concern])
                                       session-opts-valid?))

(s/fdef di/make-client-session-options
  :args (s/cat :options ::client-session-options)
  :ret session-options?)

(defn collation?
  [object]
  (instance? Collation object))

(s/def ::collation collation?)

(s/def ::alternate #{:non-ignorable :shifted})
(s/def ::backwards? boolean?)
(s/def ::case-first #{:lower :off :upper})
(s/def ::case-sensitive? boolean?)
(s/def ::locale ::non-blank-str)
(s/def ::max-variable #{:punct :space})
(s/def ::normalize? boolean?)
(s/def ::numeric-ordering? boolean?)
(s/def ::strength #{:identical
                    :primary
                    :quaternary
                    :secondary
                    :tertiary})

(s/def ::collation-options (s/keys :opt-un [::alternate
                                            ::backwards?
                                            ::case-first
                                            ::case-sensitive?
                                            ::locale
                                            ::max-variable
                                            ::normalize?
                                            ::numeric-ordering?
                                            ::strength]))

(s/fdef di/make-collation
  :args (s/cat :options ::collation-options)
  :ret (s/nilable ::collation))

(s/def ::hidden? boolean?)
(s/def ::language-override (s/or :str ::non-blank-str :kw simple-keyword?))
(s/def ::sphere-version ::integer)
(s/def :bson/storage-engine ::bson)
(s/def ::text-version ::integer)
(s/def :bson/weights ::bson)
(s/def :bson/wildcard-projection ::bson)

(s/def ::make-index-options (s/keys :opt-un [::background?
                                             ::bits
                                             ::collation-options
                                             ::default-language
                                             ::expire-after-seconds
                                             ::hidden?
                                             ::language-override
                                             ::index-name
                                             :bson/partial-filter-expression
                                             ::sparse?
                                             ::sphere-version
                                             :bson/storage-engine
                                             ::text-version
                                             ::unique?
                                             ::version
                                             :bson/weights
                                             :bson/wildcard-projection]))

(defn index-options?
  [object]
  (instance? IndexOptions object))

(s/fdef di/make-index-options
  :args (s/cat :options ::make-index-options)
  :ret index-options?)

(s/fdef di/make-index-bson
  :args (s/cat :index-keys ::index-keys)
  :ret ::bson)

(s/def ::array-filters (s/coll-of ::bson :into []))
(s/def ::bypass-validation? boolean?)
(s/def ::comment ::non-blank-str)
(s/def :bson/hint ::bson)
(s/def :bson/variables ::bson)

(s/def ::make-update-options (s/keys :opt-un [::array-filters
                                              ::bypass-validation?
                                              ::collation-options
                                              ::comment
                                              :bson/hint
                                              ::upsert?
                                              :bson/variables]))

(defn update-options?
  [object]
  (instance? UpdateOptions object))

(s/fdef di/make-update-options
  :args (s/cat :options ::make-update-options)
  :ret (s/nilable update-options?))

(s/def ::make-replace-options (s/keys :opt-un [::bypass-validation?
                                               ::collation-options
                                               ::comment
                                               :bson/hint
                                               ::upsert?
                                               :bson/variables]))

(defn replace-options?
  [object]
  (instance? ReplaceOptions object))

(s/fdef di/make-replace-options
  :args (s/cat :options ::make-replace-options)
  :ret (s/nilable replace-options?))

(s/def :bson/projection ::bson)
(s/def ::return #{:after :before})
(s/def :bson/sort ::bson)

(s/def ::make-find-one-and-delete-options (s/keys :opt-un [::collation-options
                                                           ::comment
                                                           :bson/hint
                                                           ::max-time-ms
                                                           :bson/projection
                                                           ::return
                                                           :bson/sort
                                                           ::upsert?
                                                           :bson/variables]))

(defn find-one-and-delete-options?
  [object]
  (instance? FindOneAndDeleteOptions object))

(s/fdef di/make-find-one-and-delete-options
  :args (s/cat :options ::make-find-one-and-delete-options)
  :ret (s/nilable find-one-and-delete-options?))

(s/def ::make-find-one-and-replace-options (s/keys :opt-un [::bypass-validation?
                                                            ::collation-options
                                                            ::comment
                                                            :bson/hint
                                                            ::max-time-ms
                                                            :bson/projection
                                                            ::return
                                                            :bson/sort
                                                            ::upsert?
                                                            :bson/variables]))

(defn find-one-and-replace-options?
  [object]
  (instance? FindOneAndReplaceOptions object))

(s/fdef di/make-find-one-and-replace-options
  :args (s/cat :options ::make-find-one-and-replace-options)
  :ret (s/nilable find-one-and-replace-options?))

(s/def ::make-find-one-and-update-options (s/keys :opt-un [::array-filters
                                                           ::bypass-validation?
                                                           ::collation-options
                                                           ::comment
                                                           :bson/hint
                                                           ::max-time-ms
                                                           :bson/projection
                                                           ::return
                                                           :bson/sort
                                                           ::upsert?
                                                           :bson/variables]))

(defn find-one-and-update-options?
  [object]
  (instance? FindOneAndUpdateOptions object))

(s/fdef di/make-find-one-and-update-options
  :args (s/cat :options ::make-find-one-and-update-options)
  :ret (s/nilable find-one-and-update-options?))

(s/def ::make-count-options (s/keys :opt-un [::collation-options
                                             ::comment
                                             :bson/hint
                                             ::limit
                                             ::max-time-ms
                                             ::skip]))

(defn count-options?
  [object]
  (instance? CountOptions object))

(s/fdef di/make-count-options
  :args (s/cat :options ::make-count-options)
  :ret (s/nilable count-options?))
