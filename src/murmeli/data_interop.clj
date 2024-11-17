(ns murmeli.data-interop
  (:require [murmeli.convert :as c])
  (:import [com.mongodb Block
                        ClientSessionOptions
                        ConnectionString
                        MongoClientSettings
                        MongoCredential
                        ReadConcern
                        ReadPreference
                        ServerAddress
                        ServerApi
                        ServerApiVersion
                        TransactionOptions
                        WriteConcern]
           [com.mongodb.client.model Collation
                                     CollationAlternate
                                     CollationCaseFirst
                                     CollationMaxVariable
                                     CollationStrength
                                     CountOptions
                                     FindOneAndDeleteOptions
                                     FindOneAndReplaceOptions
                                     FindOneAndUpdateOptions
                                     IndexOptions
                                     Indexes
                                     ReplaceOptions
                                     ReturnDocument
                                     UpdateOptions]
           [com.mongodb.connection ClusterSettings$Builder SslSettings$Builder]
           [java.util List]
           [java.util.concurrent TimeUnit]
           [org.bson.conversions Bson]))

(set! *warn-on-reflection* true)

(defn get-read-preference
  ^ReadPreference
  [choice]
  (case choice
    :nearest             (ReadPreference/nearest)
    :primary             (ReadPreference/primary)
    :secondary           (ReadPreference/secondary)
    :primary-preferred   (ReadPreference/primaryPreferred)
    :secondary-preferred (ReadPreference/secondaryPreferred)))

(defn get-read-concern
  ^ReadConcern
  [choice]
  (case choice
    :available    ReadConcern/AVAILABLE
    :local        ReadConcern/LOCAL
    :linearizable ReadConcern/LINEARIZABLE
    :snapshot     ReadConcern/SNAPSHOT
    :majority     ReadConcern/MAJORITY
    :default      ReadConcern/DEFAULT))

(defn get-write-concern
  ^WriteConcern
  [choice]
  (case choice
    :w1             WriteConcern/W1
    :w2             WriteConcern/W2
    :w3             WriteConcern/W3
    :majority       WriteConcern/MAJORITY
    :journaled      WriteConcern/JOURNALED
    :acknowledged   WriteConcern/ACKNOWLEDGED
    :unacknowledged WriteConcern/UNACKNOWLEDGED))

(defn- apply-block
  [f]
  (reify
    Block
    (apply [_ arg]
      (f arg))))

(def ^:private api-version ServerApiVersion/V1)

(defn make-hosts
  [hosts]
  (->> hosts
       (mapv (fn [{:keys [^String host ^Long port]}]
               (if port
                 (ServerAddress. host port)
                 (ServerAddress. host))))))

(defn make-client-settings
  ^MongoClientSettings
  [{:keys                      [^String uri
                                credentials
                                ssl-settings
                                cluster-settings
                                read-concern
                                write-concern
                                read-preference
                                retry-reads?
                                retry-writes?
                                keywords?]
    {:keys [username
            auth-db
            ^String password]} :credentials}]
  (cond-> (MongoClientSettings/builder)
    true                  (.serverApi (-> (ServerApi/builder)
                                          (.version api-version)
                                          .build))
    uri                   (.applyConnectionString (ConnectionString. uri))
    (some? retry-reads?)  (.retryReads (boolean retry-reads?))
    (some? retry-writes?) (.retryWrites (boolean retry-writes?))
    read-concern          (.readConcern (get-read-concern read-concern))
    write-concern         (.writeConcern (get-write-concern write-concern))
    read-preference       (.readPreference (get-read-preference read-preference))
    (seq credentials)     (.credential (MongoCredential/createScramSha256Credential username auth-db (.toCharArray password)))
    cluster-settings      (.applyToClusterSettings (apply-block (fn [^ClusterSettings$Builder cluster-builder]
                                                                  (let [{:keys [hosts]} cluster-settings]
                                                                    (when (seq hosts)
                                                                      (.hosts cluster-builder (make-hosts hosts)))))))
    ssl-settings          (.applyToSslSettings (apply-block (fn [^SslSettings$Builder ssl-builder]
                                                              (let [{:keys [enabled?
                                                                            invalid-hostname-allowed?]} ssl-settings]
                                                                (when (some? enabled?)
                                                                  (.enabled ssl-builder (boolean enabled?)))
                                                                (when (some? invalid-hostname-allowed?)
                                                                  (.invalidHostNameAllowed ssl-builder (boolean invalid-hostname-allowed?)))))))
    true                  (.codecRegistry (c/registry {:keywords? keywords?}))
    true                  (.build)))

;; See https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
(defn make-client-session-options
  ^ClientSessionOptions
  [{:keys [causally-consistent?
           default-timeout-ms
           read-concern
           read-preference
           snapshot?
           write-concern]}]
  (cond-> (ClientSessionOptions/builder)
    (some? causally-consistent?) (.causallyConsistent (boolean causally-consistent?))
    default-timeout-ms           (.defaultTimeout (long default-timeout-ms) TimeUnit/MILLISECONDS)
    (some? snapshot?)            (.snapshot (boolean snapshot?))
    ;; https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/
    (or read-preference
        read-concern
        write-concern)           (.defaultTransactionOptions (cond-> (TransactionOptions/builder)
                                                               read-preference (.readPreference (get-read-preference read-preference))
                                                               read-concern    (.readConcern (get-read-concern read-concern))
                                                               write-concern   (.writeConcern (get-write-concern write-concern))
                                                               true            .build))
    true                         (.build)))

(defn make-collation
  [{:keys [alternate
           backwards?
           case-first
           case-sensitive?
           locale
           max-variable
           normalize?
           numeric-ordering?
           strength]
    :as   options}]
  (when (seq options)
    (cond-> (Collation/builder)
      alternate                 (.collationAlternate (case alternate
                                                       :non-ignorable CollationAlternate/NON_IGNORABLE
                                                       :shifted       CollationAlternate/SHIFTED))
      (some? backwards?)        (.backwards (boolean backwards?))
      case-first                (.collationCaseFirst (case case-first
                                                       :lower CollationCaseFirst/LOWER
                                                       :off   CollationCaseFirst/OFF
                                                       :upper CollationCaseFirst/UPPER))
      (some? case-sensitive?)   (.caseLevel (boolean case-sensitive?))
      locale                    (.locale locale)
      max-variable              (.collationMaxVariable (case max-variable
                                                         :punct CollationMaxVariable/PUNCT
                                                         :space CollationMaxVariable/SPACE))
      (some? normalize?)        (.normalization (boolean normalize?))
      (some? numeric-ordering?) (.numericOrdering (boolean numeric-ordering?))
      strength                  (.collationStrength (case strength
                                                      :identical  CollationStrength/IDENTICAL
                                                      :primary    CollationStrength/PRIMARY
                                                      :quaternary CollationStrength/QUATERNARY
                                                      :secondary  CollationStrength/SECONDARY
                                                      :tertiary   CollationStrength/TERTIARY))
      true                      (.build))))

(defn make-index-options
  ^IndexOptions
  [{:keys [background?
           bits
           collation-options
           default-language
           expire-after-seconds
           hidden?
           language-override
           max-boundary
           min-boundary
           name
           partial-filter-expression
           sparse?
           sphere-version
           storage-engine
           text-version
           unique?
           version
           weights
           wildcard-projection]}]
  (let [collation (when (seq collation-options)
                    (make-collation collation-options))]
    (cond-> (IndexOptions.)
      (some? background?)       (.background (boolean background?))
      bits                      (.bits (int bits))
      collation                 (.collation collation)
      default-language          (.defaultLanguage default-language)
      expire-after-seconds      (.expireAfter (long expire-after-seconds) TimeUnit/SECONDS)
      (some? hidden?)           (.hidden (boolean hidden?))
      language-override         (.languageOverride language-override)
      max-boundary              (.max (double max-boundary))
      min-boundary              (.min (double min-boundary))
      name                      (.name name)
      partial-filter-expression (.partialFilterExpression partial-filter-expression)
      (some? sparse?)           (.sparse (boolean sparse?))
      sphere-version            (.sphereVersion (int sphere-version))
      storage-engine            (.storageEngine storage-engine)
      text-version              (.textVersion (int text-version))
      (some? unique?)           (.unique (boolean unique?))
      version                   (.version (int version))
      weights                   (.weights weights)
      wildcard-projection       (.wildcardProjection wildcard-projection))))

(defn make-index-bson
  ^Bson [index-keys]
  (let [^List indexes (->> index-keys
                           (mapv (fn [[field-name index-type]]
                                   (let [field-name        (name field-name)
                                         ^List field-names [field-name]]
                                     (case index-type
                                       "2d"       (Indexes/geo2d field-name)
                                       "2dsphere" (Indexes/geo2dsphere field-names)
                                       "text"     (Indexes/text field-name)
                                       1          (Indexes/ascending field-names)
                                       -1         (Indexes/descending field-names))))))]
    (if (= 1 (count indexes))
      (first indexes)
      (Indexes/compoundIndex indexes))))

(defn make-update-options
  ^UpdateOptions
  [{:keys [array-filters
           bypass-validation?
           collation-options
           ^String comment
           hint
           upsert?
           variables]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (UpdateOptions.)
        (seq array-filters)        (.arrayFilters array-filters)
        (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
        collation                  (.collation collation)
        comment                    (.comment comment)
        hint                       (.hint hint)
        (some? upsert?)            (.upsert (boolean upsert?))
        variables                  (.let variables)))))

(defn make-replace-options
  ^ReplaceOptions
  [{:keys [bypass-validation?
           collation-options
           ^String comment
           hint
           upsert?
           variables]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (ReplaceOptions.)
        (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
        collation                  (.collation collation)
        comment                    (.comment comment)
        hint                       (.hint hint)
        (some? upsert?)            (.upsert (boolean upsert?))
        variables                  (.let variables)))))

(defn make-find-one-and-delete-options
  ^FindOneAndDeleteOptions
  [{:keys [collation-options
           ^String comment
           max-time-ms
           hint
           projection
           sort
           variables]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (FindOneAndDeleteOptions.)
        collation   (.collation collation)
        comment     (.comment comment)
        max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
        hint        (.hint hint)
        projection  (.projection projection)
        sort        (.sort sort)
        variables   (.let variables)))))

(defn make-find-one-and-replace-options
  ^FindOneAndReplaceOptions
  [{:keys [bypass-validation?
           collation-options
           ^String comment
           max-time-ms
           hint
           projection
           return
           sort
           upsert?
           variables]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (FindOneAndReplaceOptions.)
        (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
        collation                  (.collation collation)
        comment                    (.comment comment)
        max-time-ms                (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
        hint                       (.hint hint)
        projection                 (.projection projection)
        sort                       (.sort sort)
        return                     (.returnDocument (case return
                                                      :after  ReturnDocument/AFTER
                                                      :before ReturnDocument/BEFORE))
        (some? upsert?)            (.upsert upsert?)
        variables                  (.let variables)))))

(defn make-find-one-and-update-options
  ^FindOneAndUpdateOptions
  [{:keys [array-filters
           bypass-validation?
           collation-options
           ^String comment
           hint
           max-time-ms
           projection
           return
           sort
           upsert?
           variables]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (FindOneAndUpdateOptions.)
        (seq array-filters)        (.arrayFilters array-filters)
        (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
        collation                  (.collation collation)
        comment                    (.comment comment)
        hint                       (.hint hint)
        max-time-ms                (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
        projection                 (.projection projection)
        return                     (.returnDocument (case return
                                                      :after  ReturnDocument/AFTER
                                                      :before ReturnDocument/BEFORE))
        sort                       (.sort sort)
        (some? upsert?)            (.upsert upsert?)
        variables                  (.let variables)))))

(defn make-count-options
  ^CountOptions
  [{:keys [collation-options
           ^String comment
           hint
           limit
           max-time-ms
           skip]
    :as   options}]
  (when (seq options)
    (let [collation (when (seq collation-options)
                      (make-collation collation-options))]
      (cond-> (CountOptions.)
        collation   (.collation collation)
        comment     (.comment comment)
        hint        (.hint hint)
        limit       (.limit (int limit))
        max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
        skip        (.skip (int skip))))))
