(ns murmeli.core
  "Murmeli MongoDB driver"
  (:require [clojure.tools.logging :as log]
            [murmeli.impl.client :as client]
            [murmeli.impl.collection :as collection]
            [murmeli.impl.convert :as c]
            [murmeli.impl.db :as db])
  (:import [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

;; Utilities

(defn create-object-id
  "Returns a new unique `org.bson.types.ObjectId` instance."
  []
  (ObjectId/get))

(defn create-id
  "Returns a new unique string representation of an object id."
  []
  (str (create-object-id)))

(defn id?
  "Returns true if given string represents an object id."
  [id]
  (c/id? id))

(defn object-id?
  "Returns true if given object is an `org.bson.types.ObjectId`."
  [id]
  (instance? ObjectId id))

;; Connect and disconnect

(defn connect-client!
  "Connects to a Mongo instance as described by the `db-spec` by constructing a client connection.

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `cluster-settings` -- Map of cluster settings, see below
  * `credentials` -- Credentials to use, map of `auth-db`, `username`, and `password`
  * `keywords?` -- If true, deserialize map keys as keywords instead of strings
  * `read-concern` -- Choose level of read isolation, see [[murmeli.impl.data-interop/get-read-concern]]
  * `read-preference` -- Choose preferred replica set members when reading, see [[murmeli.impl.data-interop/get-read-preference]]
  * `retry-reads?` -- Retry reads if they fail due to a network error
  * `retry-writes?` -- Retry writes if they fail due to a network error
  * `ssl-settings` -- Map of SSL settings, see below
  * `uri` -- The connection string to use, eg. \"mongodb://[username:password@]host[:port1],...\"
  * `write-concern` -- Acknowledgement of write operations, see [[murmeli.impl.data-interop/get-write-concern]]

  The `cluster-settings` map:
  * `hosts` -- Sequence of maps with `host` and optionally `port`

  The `ssl-settings` map:
  * `enabled?` -- Enable SSL
  * `invalid-hostname-allowed?` -- Allow invalid hostnames

  Returns a connection (map)."
  {:arglists '([{:keys [cluster-settings
                        credentials
                        keywords?
                        read-concern
                        read-preference
                        retry-reads?
                        retry-writes?
                        ssl-settings
                        uri
                        write-concern]
                 :as   db-spec}])}
  [db-spec]
  (client/connect-client! db-spec))

(defn connected?
  "Return `true` if the `conn` contains a client connection."
  [conn]
  (client/connected? conn))

(defn disconnect!
  "Disconnect the client."
  [conn]
  (client/disconnect! conn))

;; Databases

(defn with-db
  "Retrieve a database using the client and store it in the connection."
  [conn database-name]
  {:pre [database-name]}
  (db/with-db conn database-name))

(defn with-registry
  [conn registry]
  (db/with-registry conn registry))

(defn with-default-registry
  [conn]
  (db/with-default-registry conn))

(defn list-db-names-reducible
  "Query all the database names."
  {:arglists '([conn & {:keys [batch-size]}])}
  [conn & {:as options}]
  (client/list-db-names-reducible conn options))

(defn list-db-names
  {:arglists '([conn & {:keys [batch-size
                               keywords?]}])}
  [conn & {:as options}]
  (let [names (client/list-db-names conn options)]
    (log/debugf "Database names query found %d names." (count names))
    names))

(defn list-dbs-reducible
  "Query all the databases as documents."
  {:arglists '([conn & {:keys [authorized-databases?
                               batch-size
                               comment
                               max-time-ms
                               name-only?
                               query
                               timeout-mode]}])}
  [conn & {:as options}]
  (client/list-dbs-reducible conn options))

(defn list-dbs
  "Query all the databases as documents.
  Returns a vector of maps, where each map contains keys like `:name`, `:sizeOnDisk`, `:empty`."
  {:arglists '([conn & {:keys [authorized-databases?
                               batch-size
                               comment
                               max-time-ms
                               name-only?
                               query
                               timeout-mode]}])}
  [conn & {:as options}]
  (let [documents (into [] (client/list-dbs-reducible conn options))]
    (log/debugf "Database query found '%s' documents." (count documents))
    documents))

(defn drop-db!
  "Drop the given database.
  Does nothing, if the database does not exist.
  Returns `nil`."
  [conn database-name]
  (db/drop-db! conn database-name)
  (log/debugf "Dropped database '%s'." database-name))

;; Collections

(defn create-collection!
  "Creates a collection.
  Returns `nil`."
  [conn collection & {:as options}]
  (db/create-collection! conn collection options)
  (log/debugf "Created collection '%s'." collection))

(defn list-collection-names-reducible
  "Query the collections names in the database.

  Options:
  * `authorized-collections?` -- Allows querying collections names without the `listCollections` privilege
  * `batch-size` -- Number of documents per batch
  * `comment` -- Comment for this operation
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `query` -- Query filter

  Returns a reducible ([IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java)),
  which can be reduced (using `reduce`, `into`, `transduce`, `run!`..) to execute the query and produce the collection names."
  {:arglists '([conn & {:keys [authorized-collections?
                               batch-size
                               comment
                               max-time-ms
                               query]}])}
  [conn & {:as options}]
  (db/list-collection-names-reducible conn options))

(defn list-collection-names
  "Query the collections names in the database.

  Options:
  * `authorized-collections?` -- Allows querying collections names without the `listCollections` privilege
  * `batch-size` -- Number of documents per batch
  * `comment` -- Comment for this operation
  * `keywords?` -- If true, return collection names as keywords
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `query` -- Query filter

  Returns a set of collection names."
  {:arglists '([conn & {:keys [batch-size
                               max-time-ms
                               keywords?]}])}
  [conn & {:as options}]
  (let [documents (db/list-collection-names conn options)]
    (log/debugf "Collection names query found %d collections." (count documents))
    documents))

(defn list-collections-reducible
  "Query all the collections as documents."
  {:arglists '([conn & {:keys [batch-size
                               comment
                               max-time-ms
                               query
                               timeout-mode]}])}
  [conn & {:as options}]
  (db/list-collections-reducible conn options))

(defn list-collections
  "Query all the collections as documents.
  Returns a vector of maps, where each map contains keys like `:name`, `:sizeOnDisk`, `:empty`."
  {:arglists '([conn & {:keys [batch-size
                               comment
                               max-time-ms
                               query
                               timeout-mode]}])}
  [conn & {:as options}]
  (let [documents (into [] (db/list-collections-reducible conn options))]
    (log/debugf "Collections query found '%s' documents." (count documents))
    documents))

(defn drop-collection!
  "Drop the given collection from the database.

  Returns `nil`."
  [conn collection]
  (collection/drop-collection! conn collection)
  (log/debugf "Dropped collection '%s'." collection))

;; Transactions / Sessions

(defn with-client-session-options
  "Store session options into `conn`, read by `with-session`"
  {:arglists '([conn & {:keys [causally-consistent?
                               default-timeout-ms
                               read-concern
                               read-preference
                               snapshot?
                               write-concern]}])}
  [conn & {:as options}]
  (client/with-client-session-options conn options))

(defmacro with-session
  "Run `body` in a session/transaction.

  Gets the session options (as set by `with-client-session-options`), starts a session and stores it
  in `conn` and binds the result to `sym`. The session/transaction is either committed or aborted
  depending on whether `body` throws or not."
  {:arglists '([[sym conn :as bindings] & body])}
  [bindings & body]
  (client/with-session* bindings body))

;; Indexes

(defn create-index!
  "Create a new index.

  Arguments:
  * `conn` -- The database connection
  * `collection` -- The collection to operate on
  * `index-keys` -- Map of index name to index type (1, -1, 2d, 2dsphere, text)

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `background?` -- Create index in the background
  * `bits` -- 2d index location geodata hash precision bits
  * `collation-options` -- Map of collation options, see [[murmeli.impl.data-interop/make-collation]]
  * `default-language` -- Text index language
  * `expire-after-seconds` -- TTL for removing indexed documents
  * `hidden?` -- Hide index from the query planner
  * `language-override` -- Name of the field that contains the language string
  * `max-boundary` -- Upper inclusive boundary for the longitude and latitude values for 2d indexes
  * `min-boundary` -- Lower inclusive boundary for the longitude and latitude values for 2d indexes
  * `index-name` -- Name of the index
  * `partial-filter-expression` -- Filter expression (a map) for including documents in the index
  * `sparse?` -- Create a sparse index
  * `sphere-version` -- The 2dsphere index version number
  * `storage-engine` -- Map of storage engine options
  * `text-version` -- The text index version number
  * `unique?` -- Create a unique index
  * `version` -- The index version number
  * `weights` -- Map of text index field weights
  * `wildcard-projection` -- Map of fields to include/exclude in a wildcard index

  Returns the name of the created index."
  {:arglists '([conn
                collection
                index-keys
                & {:keys [allow-qualified?
                          background?
                          bits
                          collation-options
                          default-language
                          expire-after-seconds
                          hidden?
                          index-name
                          language-override
                          max-boundary
                          min-boundary
                          partial-filter-expression
                          sanitize-strings?
                          sparse?
                          sphere-version
                          storage-engine
                          text-version
                          unique?
                          version
                          weights
                          wildcard-projection]}])}
  [conn
   collection
   index-keys
   & {:as options}]
  (let [index-name (collection/create-index! conn collection index-keys options)]
    (log/debugf "Created index '%s' on '%s'." index-name collection)
    index-name))

(defn list-indexes
  "Query all indexes in the given collection.

  Returns a vector of maps, where each map contains keys like `:name`, `:key`,..."
  {:arglists '([conn
                collection
                & {:keys [batch-size
                          max-time-ms
                          keywords?]}])}
  [conn collection & {:as options}]
  (let [documents (collection/list-indexes conn collection options)]
    (log/debugf "Index query on '%s' found '%s' indexes." collection (count documents))
    documents))

(defn drop-all-indexes!
  "Drop all indexes (except for `_id`) in the given collection.

  Returns `nil`."
  [conn collection]
  (collection/drop-all-indexes! conn collection)
  (log/debugf "Dropped all indexes from '%s'." collection))

(defn drop-index!
  "Drop a specific index (as per the keys) in the given collection.
  Returns `nil`."
  [conn collection index-keys]
  (collection/drop-index! conn collection index-keys)
  (log/debugf "Dropped index from '%s'." collection))

(defn drop-index-by-name!
  "Drop a specific index (as per the name) in the given collection.
  Returns `nil`."
  [conn collection index-name]
  (collection/drop-index-by-name! conn collection index-name)
  (log/debugf "Dropped index '%s' from '%s'." index-name collection))

;; Insertion

(defn insert-one!
  "Insert a single document into a collection
  If the document does not contain an `_id` field, one will be generated (by default an `ObjectId`).

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `bypass-validation?` -- If true, bypass document validation
  * `comment` -- Operation comment string

  Returns a map with:
  * `id` -- `_id` of the inserted document (`String` or `ObjectId`)
  * `acknowledged?` -- True if the insertion was acknowledged"
  {:arglists '([conn collection doc & {:keys [allow-qualified?
                                              sanitize-strings?]}])}
  [conn collection doc & {:as options}]
  (let [id (collection/insert-one! conn collection doc options)]
    (log/debugf "Inserted document to '%s' with id '%s'." collection id)
    id))

(defn insert-many!
  "Insert multiple documents into a collection.
  If the documents do not contain `_id` fields, one will be generated (by default an `ObjectId`).

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `bypass-validation?` -- If true, bypass document validation
  * `comment` -- Operation comment string
  * `ordered?` -- Insert documents in the order provided, stop on the first error

  Returns a map with:
  * `ids` -- a vector containing the `_id`s of the inserted documents (`String` or `ObjectId`) in the corresponding order
  * `acknowledged?` -- True if the insertion was acknowledged"
  {:arglists '([conn collection docs & {:keys [allow-qualified?
                                               sanitize-strings?]}])}
  [conn collection docs & {:as options}]
  (let [ids (collection/insert-many! conn collection docs options)]
    (log/debugf "Inserted %d documents to '%s'." (count ids) collection)
    ids))

;; Updates

(defn update-one!
  "Find document matching `query` and apply `changes` to it.

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.impl.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns a map, where
  * `:matched` -- Number of documents matched.
  * `:modified` -- Number of documents modified.
  * `:id` -- Upserted `_id`, if any."
  {:arglists '([conn collection query changes & {:keys [allow-qualified?
                                                        array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [conn collection query changes & {:as options}]
  (let [response (collection/update-one! conn collection query changes options)]
    (log/debugf "Updated document (modified: %d, matched %d)." (:modified response) (:matched response))
    response))

(defn update-many!
  "Find document(s) matching `query` and apply `changes` to them.

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.impl.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns a map, where
  * `:matched` -- Number of documents matched.
  * `:modified` -- Number of documents modified."
  {:arglists '([conn collection query changes & {:keys [allow-qualified?
                                                        array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        sanitize-strings?
                                                        upsert?
                                                        variables]}])}
  [conn collection query changes & {:as options}]
  (let [response (collection/update-many! conn collection query changes options)]
    (log/debugf "Updated document(s) (modified: %d, matched %d)." (:modified response) (:matched response))
    response))

;; Replace

(defn replace-one!
  "Find document(s) matching `query` and replace the first one.

  Returns a map, where
  * `:matched` -- Number of documents matched.
  * `:modified` -- Number of documents modified."
  {:arglists '([conn collection query changes & {:keys [allow-qualified?
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        keywords?
                                                        sanitize-strings?
                                                        upsert?
                                                        variables]}])}
  [conn collection query replacement & {:as options}]
  (let [response (collection/replace-one! conn collection query replacement options)]
    (log/debugf "Replaced document (modified: %d, matched %d)." (:modified response) (:matched response))
    response))

;; Deletes

(defn delete-one!
  "Delete a single document based on the given collection and query.

  Returns a map, where
  * `:acknowledged?` -- True if the deletion was acknowledged.
  * `:count` -- Number of documents deleted."
  [conn collection query & {:as options}]
  (let [response (collection/delete-one! conn collection query options)]
    (log/debugf "Deleted document (acknowledged: %d, count %d)." (:acknowledged? response) (:count response))
    response))

(defn delete-many!
  "Delete document(s) based on the given collection and query.

  Returns a map, where
  * `:acknowledged?` -- True if the deletion was acknowledged.
  * `:count` -- Number of documents deleted."
  [conn collection query & {:as options}]
  (let [response (collection/delete-many! conn collection query options)]
    (log/debugf "Deleted document(s) (acknowledged: %d, count %d)." (:acknowledged? response) (:count response))
    response))

;; Queries

(defn count-collection
  "Count the number of documents in a collection.

  Returns the number of documents."
  {:arglists '([conn collection & {:keys [allow-qualified?
                                          collation-options
                                          comment
                                          hint
                                          keywords?
                                          limit
                                          max-time-ms
                                          query
                                          sanitize-strings?
                                          skip]}])}
  [conn collection & {:as options}]
  (let [c (collection/count-collection conn collection options)]
    (log/debugf "Collection '%s' has %d documents." collection c)
    c))

(defn estimated-count-collection
  "Estimate the number of documents in a collection.

  Returns the number of documents."
  [conn
   collection]
  (let [c (collection/estimated-count-collection conn collection)]
    (log/debugf "Collection '%s' has approximately %d documents." collection c)
    c))

(defn find-distinct-reducible
  "Find all distinct value of a field in a collection.

  Returns a reducible ([IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java)),
  which can be reduced (using `reduce`, `into`, `transduce`, `run!`..) to execute the query and produce the distinct values."
  {:arglists '([conn collection field & {:keys [allow-qualified?
                                                batch-size
                                                keywords?
                                                max-time-ms
                                                query
                                                sanitize-strings?]}])}
  [conn collection field & {:as options}]
  (collection/find-distinct-reducible conn collection field options))

(defn find-distinct
  "Find all distinct value of a field in a collection.

  Returns a set containing the distinct values."
  {:arglists '([conn collection field & {:keys [allow-qualified?
                                                batch-size
                                                keywords?
                                                max-time-ms
                                                query
                                                sanitize-strings?]}])}
  [conn collection field & {:as options}]
  (let [values (into #{} (collection/find-distinct-reducible conn collection field options))]
    (log/debugf "Distinct query for field '%s' in collection '%s' found %d unique values." field collection (count values))
    values))

(defn find-reducible
  "Query for documents in the given collection.

  Options:
  * `allow-qualified?` -- Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `batch-size` -- Fetch documents in N sized batches
  * `keywords?` -- Decode map keys as keywords instead of strings
  * `limit` -- Limit number of results to return
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `projection` -- Either a sequence of field names or a map of field names to projection types
  * `query` -- Map describing the query to run
  * `skip` -- Skip first N documents
  * `sort` -- Map of field name to sort type

  Returns a reducible ([IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java)),
  which can be reduced (using `reduce`, `into`, `transduce`, `run!`..). to execute the query and produce the matched documents."
  {:arglists '([conn collection & {:keys [allow-qualified?
                                          batch-size
                                          keywords?
                                          limit
                                          max-time-ms
                                          projection
                                          query
                                          sanitize-strings?
                                          skip
                                          sort]}])}
  [conn collection & {:as options}]
  (collection/find-reducible conn collection options))

(defn find-all
  "Like [[find-reducible]], but eagerly realizes all matches into a vector."
  {:arglists '([conn collection & {:keys [allow-qualified?
                                          batch-size
                                          keywords?
                                          limit
                                          max-time-ms
                                          projection
                                          query
                                          sanitize-strings?
                                          skip
                                          sort]}])}
  [conn collection & {:as options}]
  (let [documents (into [] (collection/find-reducible conn collection options))]
    (log/debugf "Query for collection '%s' found %d documents." collection (count documents))
    documents))

(defn find-one
  "Like [[find-all]], but fetches a single document

  By default will warn & throw if the query produces more than one document."
  {:arglists '([conn collection & {:keys [allow-qualified?
                                          keywords?
                                          projection
                                          query
                                          sanitize-strings?
                                          throw-on-multiple?
                                          warn-on-multiple?]}])}
  [conn collection & {:as options}]
  (let [documents (collection/find-one conn collection options)]
    (log/debugf "Find-one query for collection '%s' found %d documents." collection (count documents))
    documents))

(defn find-by-id
  "Like [[find-one]], but fetches a single document by id."
  {:arglists '([conn collection id & {:keys [allow-qualified?
                                             keywords?
                                             projection
                                             sanitize-strings?]}])}
  [conn collection id & {:as options}]
  (collection/find-by-id conn collection id options))

;; Find one and - API

(defn find-one-and-delete!
  "Find a document and remove it.

  Returns the document, or `nil` if none found."
  {:arglists '([conn collection query & {:keys [allow-qualified?
                                                collation-options
                                                comment
                                                hint
                                                keywords?
                                                max-time-ms
                                                projection
                                                sanitize-strings?
                                                sort
                                                variables]}])}
  [conn collection query & {:as options}]
  (collection/find-one-and-delete! conn collection query options))

(defn find-one-and-replace!
  "Find a document and replace it.

  Returns the document, or `nil` if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  {:arglists '([conn collection query replacement & {:keys [allow-qualified?
                                                            bypass-validation?
                                                            collation-options
                                                            comment
                                                            hint
                                                            keywords?
                                                            max-time-ms
                                                            projection
                                                            return
                                                            sanitize-strings?
                                                            sort
                                                            upsert?
                                                            variables]}])}
  [conn collection query replacement & {:as options}]
  (collection/find-one-and-replace! conn collection query replacement options))

(defn find-one-and-update!
  "Find a document and update it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  {:arglists '([conn collection query updates & {:keys [allow-qualified?
                                                        array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        keywords?
                                                        max-time-ms
                                                        projection
                                                        return
                                                        sanitize-strings?
                                                        sort
                                                        upsert?
                                                        variables]}])}
  [conn collection query updates & {:as options}]
  (collection/find-one-and-update! conn collection query updates options))

;; Aggregation

(defn aggregate-reducible!
  "Execute an aggregation pipelien on a collection.

  Returns a reducible ([IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java)),
  which can be reduced (using `reduce`, `into`, `transduce`, `run!`,...)
  to execute the aggregation and produce the resulting documents."
  [conn collection pipeline & { :as options}]
  (collection/aggregate-reducible! conn collection pipeline options))

(defn aggregate!
  "Like [[aggregate-reducible!]], but eagerly executes the aggregation and returns a vector of documents."
  [conn collection pipeline & { :as options}]
  (let [documents (into [] (collection/aggregate-reducible! conn collection pipeline options))]
    (log/debugf "Aggregation query for collection '%s' produced %d documents." collection (count documents))
    documents))
