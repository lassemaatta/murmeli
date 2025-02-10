(ns murmeli.core
  "Murmeli MongoDB driver

  [Javadoc](https://www.mongodb.com/docs/drivers/java/sync/current/)"
  (:require [murmeli.impl.client :as client]
            [murmeli.impl.collection :as collection]
            [murmeli.impl.convert :as c]
            [murmeli.impl.cursor]
            [murmeli.impl.db :as db]
            [murmeli.impl.index :as index]
            [murmeli.impl.query :as query]
            [murmeli.impl.session :as session])
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

  Returns a connection (map).

  Options:
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
  * `invalid-hostname-allowed?` -- Allow invalid hostnames"
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
                 :as db-spec}])}
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

(defn list-dbs
  "List all databases as documents.
  Returned documents contain keys like `:name`, `:sizeOnDisk`, `:empty`."
  [conn]
  (db/list-dbs conn))

(defn drop-db!
  "Drop the given database.
  Does nothing, if the database does not exist. Returns `nil`."
  [conn database-name]
  (db/drop-db! conn database-name))

;; Collections

(defn create-collection!
  "Creates a collection."
  [conn collection]
  (collection/create-collection! conn collection))

(defn list-collection-names
  "Returns a set of collection names in the current database.

  Options:
  * `batch-size` -- Number of documents per batch
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `keywords?` -- If true, return collection names as keywords"
  {:arglists '([conn & {:keys [batch-size
                               max-time-ms
                               keywords?]}])}
  [conn & {:as options}]
  (collection/list-collection-names conn options))

(defn drop-collection!
  "Drop the given collection from the database"
  [conn collection]
  (collection/drop-collection! conn collection))

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
  (session/with-client-session-options conn options))

(defmacro with-session
  "Run `body` in a session/transaction.

  Gets the session options (as set by `with-client-session-options`), starts a session and stores it
  in `conn` and binds the result to `sym`. The session/transaction is either committed or aborted
  depending on whether `body` throws or not."
  {:arglists '([[sym conn :as bindings] & body])}
  [bindings & body]
  (session/with-session* bindings body))

;; Indexes

(defn create-index!
  "Create a new index.

  Arguments:
  * `conn` -- The database connection
  * `collection` -- The collection to operate on
  * `index-keys` -- Map of index name to index type (1, -1, 2d, 2dsphere, text)

  Options:
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
  * `wildcard-projection` -- Map of fields to include/exclude in a wildcard index"
  {:arglists '([conn
                collection
                index-keys
                & {:keys [background?
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
                          wildcard-projection]}])}
  [conn
   collection
   index-keys
   & {:as options}]
  (index/create-index! conn collection index-keys options))

(defn list-indexes
  "List indexes in the given collection."
  {:arglists '([conn
                collection
                & {:keys [batch-size
                          max-time-ms
                          keywords?]}])}
  [conn collection & {:as options}]
  (index/list-indexes conn collection options))

(defn drop-all-indexes!
  [conn collection]
  (index/drop-all-indexes! conn collection))

(defn drop-index!
  [conn collection index-keys]
  (index/drop-index! conn collection index-keys))

(defn drop-index-by-name!
  [conn collection index-name]
  (index/drop-index-by-name! conn collection index-name))

;; Insertion

(defn insert-one!
  "Insert a single document into a collection
  If the document does not contain an `_id` field, one will be generated (by default an `ObjectId`).
  Returns the `_id` of the inserted document (`String` or `ObjectId`)."
  [conn collection doc]
  (query/insert-one! conn collection doc))

(defn insert-many!
  "Insert multiple documents into a collection.
  If the documents do not contain `_id` fields, one will be generated (by default an `ObjectId`).
  Returns the `_id`s of the inserted documents (`String` or `ObjectId`) in the corresponding order."
  [conn collection docs]
  (query/insert-many! conn collection docs))

;; Updates

(defn update-one!
  "Find document matching `query` and apply `changes` to it.

  Options:
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.impl.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([conn collection query changes & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [conn collection query changes & {:as options}]
  (query/update-one! conn collection query changes options))

(defn update-many!
  "Find document(s) matching `query` and apply `changes` to them.

  Options:
  * `array-filters` -- List of array filter documents
  * `bypass-validation?` -- If true, bypass document validation
  * `collation-options` -- Map of collation options, see [[murmeli.impl.data-interop/make-collation]]
  * `comment` -- Operation comment string
  * `hint` -- Indexing hint document
  * `upsert?` -- If true, insert `changes` document if no existing document matches `query`
  * `variables` -- Top-level variable documents

  Returns the number of matched and updated documents."
  {:arglists '([conn collection query changes & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [conn collection query changes & {:as options}]
  (query/update-many! conn collection query changes options))

;; Replace

(defn replace-one!
  "Find document(s) matching `query` and replace the first one.
  Returns a map describing if a match was found and if it was actually altered."
  {:arglists '([conn collection query changes & {:keys [bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        upsert?
                                                        variables]}])}
  [conn collection query replacement & {:as options}]
  (query/replace-one! conn collection query replacement options))

;; Deletes

;; TODO tests docstring
(defn delete-one!
  [conn collection query]
  (query/delete-one! conn collection query))

;; TODO tests docstring
(defn delete-many!
  [conn collection query]
  (query/delete-many! conn collection query))

;; Queries

(defn count-collection
  "Count the number of documents in a collection"
  {:arglists '([conn collection & {:keys [query
                                          collation-options
                                          comment
                                          hint
                                          limit
                                          max-time-ms
                                          skip]}])}
  [conn collection & {:as options}]
  (query/count-collection conn collection options))

(defn estimated-count-collection
  "Gets an estimate of the count of documents in a collection using collection metadata."
  [conn
   collection]
  (query/estimated-count-collection conn collection))

(defn find-distinct-reducible
  "Find all distinct value of a field in a collection. Returns a set."
  {:arglists '([conn collection field & {:keys [batch-size
                                                keywords?
                                                max-time-ms
                                                query]}])}
  [conn collection field & {:as options}]
  (query/find-distinct-reducible conn collection field options))

(defn find-distinct
  "Find all distinct value of a field in a collection. Returns a set."
  {:arglists '([conn collection field & {:keys [query
                                                batch-size
                                                max-time-ms
                                                keywords?]}])}
  [conn collection field & {:as options}]
  (query/find-distinct conn collection field options))

(defn find-reducible
  "Query for documents in the given collection.

  Options:
  * `batch-size` -- Fetch documents in N sized batches
  * `keywords?` -- Decode map keys as keywords instead of strings
  * `limit` -- Limit number of results to return
  * `max-time-ms` -- Maximum execution time on server in milliseconds
  * `projection` -- Either a sequence of field names or a map of field names to projection types
  * `query` -- Map describing the query to run
  * `skip` -- Skip first N documents
  * `sort` -- Map of field name to sort type

  Returns a reducible (`IReduceInit`) that eagerly runs the query when reduced with a function
  (using `reduce`, `into`, `transduce`, `run!`..)."
  {:arglists '([conn collection & {:keys [batch-size
                                          keywords?
                                          limit
                                          max-time-ms
                                          projection
                                          query
                                          skip
                                          sort]}])}
  [conn collection & {:as options}]
  (query/find-reducible conn collection options))

(defn find-all
  "Like `find-plan`, but eagerly realizes all matches into a vector."
  {:arglists '([conn collection & {:keys [query
                                          projection
                                          sort
                                          limit
                                          skip
                                          batch-size
                                          max-time-ms
                                          keywords?]}])}
  [conn collection & {:as options}]
  (query/find-all conn collection options))

(defn find-one
  "Like `find-all`, but fetches a single document

  By default will warn & throw if query produces more than
  one result."
  {:arglists '([conn collection & {:keys [query
                                          projection
                                          keywords?
                                          warn-on-multiple?
                                          throw-on-multiple?]}])}
  [conn collection & {:as options}]
  (query/find-one conn collection options))

(defn find-by-id
  "Like `find-one`, but fetches a single document by id."
  {:arglists '([conn collection id & {:keys [projection
                                             keywords?]}])}
  [conn collection id & {:as options}]
  (query/find-by-id conn collection id options))

;; Find one and - API

(defn find-one-and-delete!
  "Find a document and remove it.
  Returns the document, or ´nil´ if none found."
  {:arglists '([conn collection query & {:keys [collation-options
                                                comment
                                                keywords?
                                                max-time-ms
                                                hint
                                                projection
                                                sort
                                                variables]}])}
  [conn collection query & {:as options}]
  (query/find-one-and-delete! conn collection query options))

(defn find-one-and-replace!
  "Find a document and replace it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  {:arglists '([conn collection query replacement & {:keys [bypass-validation?
                                                            collation-options
                                                            comment
                                                            hint
                                                            keywords?
                                                            max-time-ms
                                                            projection
                                                            return
                                                            sort
                                                            upsert?
                                                            variables]}])}
  [conn collection query replacement & {:as options}]
  (query/find-one-and-replace! conn collection query replacement options))

(defn find-one-and-update!
  "Find a document and update it.
  Returns the document, or ´nil´ if none found. The `return` argument controls
  whether we return the document before or after the replacement."
  {:arglists '([conn collection query updates & {:keys [array-filters
                                                        bypass-validation?
                                                        collation-options
                                                        comment
                                                        hint
                                                        keywords?
                                                        max-time-ms
                                                        projection
                                                        return
                                                        sort
                                                        upsert?
                                                        variables]}])}
  [conn collection query updates & {:as options}]
  (query/find-one-and-update! conn collection query updates options))

;; Aggregation

(defn aggregate-reducible!
  [conn collection pipeline & { :as options}]
  (query/aggregate-reducible! conn collection pipeline options))

(defn aggregate!
  [conn collection pipeline & { :as options}]
  (query/aggregate! conn collection pipeline options))
