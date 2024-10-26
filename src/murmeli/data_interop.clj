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
           [com.mongodb.client.model CountOptions
                                     FindOneAndDeleteOptions
                                     FindOneAndReplaceOptions
                                     FindOneAndUpdateOptions
                                     IndexOptions
                                     Indexes
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
  [{:keys [^String uri
           credentials
           ssl-settings
           cluster-settings
           read-concern
           write-concern
           read-preference
           retry-reads?
           retry-writes?]}]
  (let [server-api (-> (ServerApi/builder)
                       (.version api-version)
                       .build)
        builder    (-> (MongoClientSettings/builder)
                       (.serverApi server-api))]
    (when (some? uri) (.applyConnectionString builder (ConnectionString. uri)))
    (when (some? retry-reads?) (.retryReads builder (boolean retry-reads?)))
    (when (some? retry-writes?) (.retryWrites builder (boolean retry-writes?)))
    (when read-concern (.readConcern builder (get-read-concern read-concern)))
    (when write-concern (.writeConcern builder (get-write-concern write-concern)))
    (when read-preference (.readPreference builder (get-read-preference read-preference)))
    (when (seq credentials)
      (let [{:keys [username auth-db ^String password]} credentials]
        (.credential builder (MongoCredential/createScramSha256Credential username auth-db (.toCharArray password)))))
    (when cluster-settings
      (.applyToClusterSettings builder (apply-block (fn [^ClusterSettings$Builder cluster-builder]
                                                      (let [{:keys [hosts]} cluster-settings]
                                                        (when (seq hosts)
                                                          (.hosts cluster-builder (make-hosts hosts))))))))
    (when ssl-settings
      (.applyToSslSettings builder (apply-block (fn [^SslSettings$Builder ssl-builder]
                                                  (let [{:keys [enabled?
                                                                invalid-hostname-allowed?]} ssl-settings]
                                                    (when (some? enabled?)
                                                      (.enabled ssl-builder (boolean enabled?)))
                                                    (when (some? invalid-hostname-allowed?)
                                                      (.invalidHostNameAllowed ssl-builder (boolean invalid-hostname-allowed?))))))))
    (.build builder)))

;; See https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/
(defn make-client-session-options
  ^ClientSessionOptions
  [{:keys [causally-consistent?
           snapshot?
           read-preference
           read-concern
           write-concern]
    :or   {causally-consistent? false
           snapshot?            false}}]
  (-> (ClientSessionOptions/builder)
      (.causallyConsistent causally-consistent?)
      ;; https://www.mongodb.com/docs/manual/reference/read-concern-snapshot/
      (.snapshot snapshot?)
      (.defaultTransactionOptions (cond-> (TransactionOptions/builder)
                                    read-preference (.readPreference (get-read-preference read-preference))
                                    read-concern    (.readConcern (get-read-concern read-concern))
                                    write-concern   (.writeConcern (get-write-concern write-concern))
                                    true            .build))
      .build))

(defn make-index-options
  ^IndexOptions
  [{:keys [background?
           name
           version
           partial-filter-expression
           unique?
           sparse?]}]
  (cond-> (IndexOptions.)
    background?               (.background true)
    name                      (.name name)
    version                   (.version (int version))
    partial-filter-expression (.partialFilterExpression (c/map->bson partial-filter-expression))
    unique?                   (.unique true)
    sparse?                   (.sparse true)))

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
  [{:keys [upsert?]}]
  (when upsert?
    (let [options (UpdateOptions.)]
      (.upsert options true)
      options)))

(defn make-find-one-and-delete-options
  ^FindOneAndDeleteOptions
  [{:keys [projection
           sort]}]
  (when (or projection sort)
    (cond-> (FindOneAndDeleteOptions.)
      projection (.projection projection)
      sort       (.sort sort))))

(defn make-find-one-and-replace-options
  ^FindOneAndReplaceOptions
  [{:keys [projection
           sort
           return
           upsert?]}]
  (when (or projection sort return (some? upsert?))
    (cond-> (FindOneAndReplaceOptions.)
      projection      (.projection projection)
      sort            (.sort sort)
      return          (.returnDocument (case return
                                         :after  ReturnDocument/AFTER
                                         :before ReturnDocument/BEFORE))
      (some? upsert?) (.upsert upsert?))))

(defn make-find-one-and-update-options
  ^FindOneAndUpdateOptions
  [{:keys [projection
           sort
           return
           max-time-ms
           upsert?]}]
  (when (or projection sort return (some? upsert?))
    (cond-> (FindOneAndUpdateOptions.)
      projection      (.projection projection)
      sort            (.sort (c/map->bson sort))
      max-time-ms     (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS)
      return          (.returnDocument (case return
                                         :after  ReturnDocument/AFTER
                                         :before ReturnDocument/BEFORE))
      (some? upsert?) (.upsert upsert?))))

(defn make-count-options
  ^CountOptions
  [{:keys [limit
           skip
           max-time-ms]}]
  (when (or limit skip max-time-ms)
    (cond-> (CountOptions.)
      limit (.limit (int limit))
      skip  (.skip (int skip))
      max-time-ms (.maxTime (long max-time-ms) TimeUnit/MILLISECONDS))))
