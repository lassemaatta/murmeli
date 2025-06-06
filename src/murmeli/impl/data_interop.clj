(ns murmeli.impl.data-interop
  "Collection of helper functions to convert clojure data to various Mongo API Java objects"
  (:require [murmeli.impl.convert :as c])
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
           [com.mongodb.client.cursor TimeoutMode]
           [com.mongodb.client.gridfs.model GridFSDownloadOptions GridFSUploadOptions]
           [com.mongodb.client.model ChangeStreamPreAndPostImagesOptions
                                     ClusteredIndexOptions
                                     Collation
                                     CollationAlternate
                                     CollationCaseFirst
                                     CollationMaxVariable
                                     CollationStrength
                                     CountOptions
                                     CreateCollectionOptions
                                     FindOneAndDeleteOptions
                                     FindOneAndReplaceOptions
                                     FindOneAndUpdateOptions
                                     IndexOptionDefaults
                                     IndexOptions
                                     Indexes
                                     InsertManyOptions
                                     InsertOneOptions
                                     ReplaceOptions
                                     ReturnDocument
                                     TimeSeriesGranularity
                                     TimeSeriesOptions
                                     UpdateOptions
                                     ValidationAction
                                     ValidationLevel
                                     ValidationOptions]
           [com.mongodb.client.model.changestream ChangeStreamDocument
                                                  FullDocument
                                                  FullDocumentBeforeChange
                                                  OperationType
                                                  TruncatedArray
                                                  UpdateDescription]
           [com.mongodb.client.result UpdateResult]
           [com.mongodb.connection ClusterSettings$Builder SslSettings$Builder]
           [java.time Instant]
           [java.util List]
           [java.util.concurrent TimeUnit]
           [org.bson Document]
           [org.bson.conversions Bson]))

(set! *warn-on-reflection* true)

(defn get-read-preference
  "Read preference represents the preferred replica set members to which queries and commands are sent."
  ^ReadPreference
  [choice]
  (case choice
    ;; Read from primary or secondary
    :nearest             (ReadPreference/nearest)
    ;; Read from primary
    :primary             (ReadPreference/primary)
    ;; Read from secondary
    :secondary           (ReadPreference/secondary)
    ;; Read from primary if available, otherwise secondary
    :primary-preferred   (ReadPreference/primaryPreferred)
    ;; Read from secondary if available, otherwise primary
    :secondary-preferred (ReadPreference/secondaryPreferred)))

(defn get-read-concern
  "Read concern represents the read isolation level."
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
  "Control the required level of acknowledgments when writing"
  ^WriteConcern
  [choice]
  (case choice
    ;; Wait for one member to acknowledge
    :w1             WriteConcern/W1
    ;; Wait for two members to acknowledge
    :w2             WriteConcern/W2
    ;; Wait for three members to acknowledge
    :w3             WriteConcern/W3
    ;; Waits on a majority of servers
    :majority       WriteConcern/MAJORITY
    ;; Wait for committed to journal file
    :journaled      WriteConcern/JOURNALED
    ;; Wait for acknowledgement
    :acknowledged   WriteConcern/ACKNOWLEDGED
    ;; Return when message written to the socket (W0)
    :unacknowledged WriteConcern/UNACKNOWLEDGED))

(defn get-timeout-mode
  [timeout-mode]
  (case timeout-mode
    :cursor-lifetime TimeoutMode/CURSOR_LIFETIME
    :iteration       TimeoutMode/ITERATION))

(defn- apply-block
  [f]
  (reify
    Block
    (apply [_ arg]
      (f arg))))

(def ^:private api-version ServerApiVersion/V1)

(defn- make-hosts
  [hosts]
  (->> hosts
       (mapv (fn [{:keys [^String host ^Long port]}]
               (if port
                 (ServerAddress. host port)
                 (ServerAddress. host))))))

(defn- make-cluster-settings
  [cluster-settings]
  (apply-block
    (fn [^ClusterSettings$Builder cluster-builder]
      (let [{:keys [hosts]} cluster-settings]
        (cond-> cluster-builder
          (seq hosts) (.hosts (make-hosts hosts)))))))

(defn- make-ssl-settings
  [ssl-settings]
  (apply-block
    (fn [^SslSettings$Builder ssl-builder]
      (let [{:keys [enabled?
                    invalid-hostname-allowed?]} ssl-settings]
        (cond-> ssl-builder
          (some? enabled?)                  (.enabled (boolean enabled?))
          (some? invalid-hostname-allowed?) (.invalidHostNameAllowed (boolean invalid-hostname-allowed?)))))))

(defn make-client-settings
  {:no-doc true}
  ^MongoClientSettings
  [{:keys                      [cluster-settings
                                credentials
                                read-concern
                                read-preference
                                retry-reads?
                                retry-writes?
                                ssl-settings
                                ^String uri
                                write-concern]
    {:keys [username
            auth-db
            ^String password]} :credentials
    :as                        options}]
  (cond-> (MongoClientSettings/builder)
    true                   (.serverApi (-> (ServerApi/builder)
                                           (.version api-version)
                                           .build))
    uri                    (.applyConnectionString (ConnectionString. uri))
    (some? retry-reads?)   (.retryReads (boolean retry-reads?))
    (some? retry-writes?)  (.retryWrites (boolean retry-writes?))
    read-concern           (.readConcern (get-read-concern read-concern))
    write-concern          (.writeConcern (get-write-concern write-concern))
    read-preference        (.readPreference (get-read-preference read-preference))
    (seq credentials)      (.credential (MongoCredential/createScramSha256Credential username auth-db (.toCharArray password)))
    (seq cluster-settings) (.applyToClusterSettings (make-cluster-settings cluster-settings))
    (seq ssl-settings)     (.applyToSslSettings (make-ssl-settings ssl-settings))
    true                   (.codecRegistry (c/registry options))
    true                   (.build)))

;; See https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
(defn make-client-session-options
  {:no-doc true}
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
        write-concern)           (.defaultTransactionOptions
                                   (cond-> (TransactionOptions/builder)
                                     read-preference (.readPreference (get-read-preference read-preference))
                                     read-concern    (.readConcern (get-read-concern read-concern))
                                     write-concern   (.writeConcern (get-write-concern write-concern))
                                     true            .build))
    true                         (.build)))

(defn make-collation
  "Construct a collation; a set of rules to compare strings.

  Options:
  * `alternate` -- How are spaces and punctuations considered:
      `:non-ignorable`; considered as base characters.
      `:shifted`; not considered as base characters, unless `strength` > 3.
  * `backwards?` -- Consider secondary differences in reverse order (e.g. French).
  * `case-first` -- Uppercase or lowercase characters first:
     `:lower`; Lowercase first.
     `:off`; Off.
     `:upper`; Uppercase first.
  * `case-sensitive?` -- If true, turn on case sensitivity.
  * `locale` -- Locale.
  * `max-variable` -- Which characters are affected by `:shifted`:
      `:punct`; Spaces and punctuation are affected.
      `:space`; Only spaces are affected.
  * `normalize?` -- If true, normalizes to Unicode NFD.
  * `numeric-ordering?` -- If true, order numbers based on numeric ordering instead of collation order.
  * `strength` -- Collation strength:
      `:identical`; When all else equal, use identical level.
      `:primary`; Strongest level.
      `:secondary`; Accents are considered secondary differences.
      `:tertiary`; Upper and lower case differences considered.
      `:quaternary`; Distinguish words with or without punctuations."
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
  {:no-doc true}
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
           index-name
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
      language-override         (.languageOverride (name language-override))
      max-boundary              (.max (double max-boundary))
      min-boundary              (.min (double min-boundary))
      index-name                (.name index-name)
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
  {:no-doc true}
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
  {:no-doc true}
  ^UpdateOptions
  [{:keys [array-filters
           bypass-validation?
           collation-options
           ^String comment
           hint
           sort
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
        sort                       (.sort sort)
        (some? upsert?)            (.upsert (boolean upsert?))
        variables                  (.let variables)))))

(defn make-replace-options
  {:no-doc true}
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
  {:no-doc true}
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
  {:no-doc true}
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
  {:no-doc true}
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
  {:no-doc true}
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

(defn make-gridfs-upload-options
  {:no-doc true}
  ^GridFSUploadOptions
  [{:keys [chunk-size-bytes
           ^Document doc]}]
  (cond-> (GridFSUploadOptions.)
    chunk-size-bytes (.chunkSizeBytes (int chunk-size-bytes))
    doc              (.metadata doc)))

(defn make-gridfs-download-options
  {:no-doc true}
  ^GridFSDownloadOptions
  [{:keys [revision]}]
  (cond-> (GridFSDownloadOptions.)
    revision (.revision (int revision))))

(defn make-change-stream-options
  ^ChangeStreamPreAndPostImagesOptions
  [{:keys [enabled?]}]
  (when (some? enabled?)
    (ChangeStreamPreAndPostImagesOptions. (boolean enabled?))))

(defn make-clustered-index-options
  ^ClusteredIndexOptions
  [{:keys [index-key
           index-name
           unique?]}]
  (when index-key
    (cond-> (ClusteredIndexOptions. index-key (boolean unique?))
      index-name (.name index-name))))

(defn make-index-option-defaults
  ^IndexOptionDefaults
  [{:keys [storage-engine]}]
  (when storage-engine
    (-> (IndexOptionDefaults.)
        (.storageEngine storage-engine))))

(defn get-granularity
  {:no-doc true}
  ^TimeSeriesGranularity [granularity]
  (case granularity
    :hours   TimeSeriesGranularity/HOURS
    :minutes TimeSeriesGranularity/MINUTES
    :seconds TimeSeriesGranularity/SECONDS))

(defn make-time-series-options
  ^TimeSeriesOptions
  [{:keys [bucket-max-span-seconds
           bucket-rounding-seconds
           granularity
           meta-field
           time-field]}]
  (when time-field
    (cond-> (TimeSeriesOptions. time-field)
      ;; Using seconds is a compromise but it looks like `TimeSeriesOptions`
      ;; internally converts the time units to seconds.
      bucket-max-span-seconds (.bucketMaxSpan (long bucket-max-span-seconds) TimeUnit/SECONDS)
      bucket-rounding-seconds (.bucketRounding (long bucket-rounding-seconds) TimeUnit/SECONDS)
      meta-field              (.metaField meta-field)
      granularity             (.granularity (get-granularity granularity)))))

(defn get-validation-action
  {:no-doc true}
  ^ValidationAction [action]
  (case action
    :error ValidationAction/ERROR
    :warn  ValidationAction/WARN))

(defn get-validation-level
  {:no-doc true}
  ^ValidationLevel [level]
  (case level
    :strict   ValidationLevel/STRICT
    :moderate ValidationLevel/MODERATE
    :off      ValidationLevel/OFF))

(defn make-validation-options
  ^ValidationOptions
  [{:keys [validation-action
           validation-level
           validator]}]
  (when (or validation-action validation-level validator)
    (cond-> (ValidationOptions.)
      validation-action (.validationAction (get-validation-action validation-action))
      validation-level  (.validationLevel (get-validation-level validation-level))
      validator         (.validator validator))))

(defn make-create-collection-options
  {:no-doc true}
  ^CreateCollectionOptions
  [{:keys [capped?
           change-stream-options
           clustered-index-options
           collation-options
           ^Bson encrypted-fields
           expire-after-seconds
           index-option-defaults-options
           max-documents
           size-in-bytes
           ^Bson storage-engine-options
           time-series-options
           validation-options]}]
  (let [change-stream   (when (seq change-stream-options)
                          (make-change-stream-options change-stream-options))
        clustered-index (when (seq clustered-index-options)
                          (make-clustered-index-options clustered-index-options))
        collation       (when (seq collation-options)
                          (make-collation collation-options))
        index-option    (when (seq index-option-defaults-options)
                          (make-index-option-defaults index-option-defaults-options))
        time-series     (when (seq time-series-options)
                          (make-time-series-options time-series-options))
        validation      (when (seq validation-options)
                          (make-validation-options validation-options))]
    (cond-> (CreateCollectionOptions.)
      (some? capped?)        (.capped (boolean capped?))
      change-stream          (.changeStreamPreAndPostImagesOptions change-stream)
      clustered-index        (.clusteredIndexOptions clustered-index)
      collation              (.collation collation)
      encrypted-fields       (.encryptedFields encrypted-fields)
      expire-after-seconds   (.expireAfter (long expire-after-seconds) TimeUnit/SECONDS)
      index-option           (.indexOptionDefaults index-option)
      max-documents          (.maxDocuments (long max-documents))
      size-in-bytes          (.sizeInBytes (long size-in-bytes))
      storage-engine-options (.storageEngineOptions storage-engine-options)
      time-series            (.timeSeriesOptions time-series)
      validation             (.validationOptions validation))))

(defn make-insert-one-options
  {:no-doc true}
  ^InsertOneOptions
  [{:keys [bypass-validation?
           ^String comment]
    :as   options}]
  (when (seq options)
    (cond-> (InsertOneOptions.)
      (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
      comment                    (.comment comment))))

(defn make-insert-many-options
  {:no-doc true}
  ^InsertManyOptions
  [{:keys [bypass-validation?
           ^String comment
           ordered?]
    :as   options}]
  (when (seq options)
    (cond-> (InsertManyOptions.)
      (some? bypass-validation?) (.bypassDocumentValidation (boolean bypass-validation?))
      comment                    (.comment comment)
      (some? ordered?)           (.ordered (boolean ordered?)))))

(defn update-result->map
  [^UpdateResult result]
  (let [id (.getUpsertedId result)]
    ;; There doesn't seem to be a way to verify that the query would match
    ;; just a single document because matched count is always either 0 or 1 :(
    (cond-> {:modified (.getModifiedCount result)
             :matched  (.getMatchedCount result)}
      id (assoc :id (c/bson-value->document-id id)))))

(defn get-full-document
  "Should the change stream contain the updated document"
  [choice]
  (case choice
    ;; Like `:when-available`, but raise an error if no document is available
    :required       FullDocument/REQUIRED
    ;; Partial updates will include a delta and the full document
    :update-lookup  FullDocument/UPDATE_LOOKUP
    ;; Return full document after modifications, if available
    :when-available FullDocument/WHEN_AVAILABLE
    FullDocument/DEFAULT))

(defn get-full-document-before-change
  "Should the change stream contain the original document"
  [choice]
  (case choice
    :default        FullDocumentBeforeChange/DEFAULT
    :off            FullDocumentBeforeChange/OFF
    :required       FullDocumentBeforeChange/REQUIRED
    :when-available FullDocumentBeforeChange/WHEN_AVAILABLE))

(def op-type->kw
  {OperationType/DELETE        :delete
   OperationType/DROP          :drop
   OperationType/DROP_DATABASE :drop-database
   OperationType/INSERT        :insert
   OperationType/INVALIDATE    :invalidate
   OperationType/OTHER         :other
   OperationType/RENAME        :rename
   OperationType/REPLACE       :replace
   OperationType/UPDATE        :update})

(defn- truncated-array
  [^TruncatedArray arr]
  {:field    (.getField arr)
   :new-size (.getNewSize arr)})

(defn- update-description
  [^UpdateDescription description bson->map]
  {:removed-fields      (into [] (.getRemovedFields description))
   :updated-fields      (some-> (.getUpdatedFields description) bson->map)
   :disambiguated-paths (some-> (.getDisambiguatedPaths description) bson->map)
   :truncated-arrays    (some->> (.getTruncatedArrays description) (mapv truncated-array))})

(defn change-stream-document
  [^ChangeStreamDocument doc bson->map]
  (let [ct          (.getClusterTime doc)
        destination (.getDestinationNamespace doc)
        namespace   (.getNamespace doc)]
    {;; BsonTimestamps have a 32 bit epoch second counter and
     ;; a 32 bit incrementing counter
     :cluster-time-inc            (.getInc ct)
     :cluster-time-s              (Instant/ofEpochSecond (.getTime ct) 0)
     :database-name               (.getDatabaseName doc)
     :destination                 {:collection (some-> destination .getCollectionName)}
     :document-key                (some-> (.getDocumentKey doc) bson->map)
     :extra-elements              (some-> (.getExtraElements doc) bson->map)
     :full-document               (.getFullDocument doc)
     :full-document-before-change (.getFullDocumentBeforeChange doc)
     :namespace                   {:database-name   (some-> namespace .getDatabaseName)
                                   :collection-name (some-> namespace .getCollectionName)}
     :lsid                        (some-> (.getLsid doc) bson->map)
     :operation-type              (get op-type->kw (.getOperationType doc) :unknown)
     :resume-token                (some-> (.getResumeToken doc) bson->map)
     :txn-number                  (some-> (.getTxnNumber doc) .getValue)
     :update-description          (some-> (.getUpdateDescription doc) (update-description bson->map))
     :wall-time                   (Instant/ofEpochMilli (.getValue (.getWallTime doc)))}))
