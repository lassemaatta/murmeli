# Murmeli, a Clojure wrapper for the MongoDB Java driver

[![Build status](https://github.com/lassemaatta/murmeli/actions/workflows/workflow.yml/badge.svg)](https://github.com/lassemaatta/murmeli/actions)

[![Clojars Project](https://img.shields.io/clojars/v/com.github.lassemaatta/murmeli.svg)](https://clojars.org/com.github.lassemaatta/murmeli)

[![cljdoc badge](https://cljdoc.org/badge/com.github.lassemaatta/murmeli)](https://cljdoc.org/d/com.github.lassemaatta/murmeli)

A relative thin wrapper around the _modern API_ of the 5.x MongoDB Java _sync_ [driver](https://www.mongodb.com/docs/drivers/java/sync/v5.3/).

Status: alpha

## Installation

Download from https://github.com/lassemaatta/murmeli.

## Features

Supports the majority of the [modern API](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/index.html).

(Proof-of-Concept) Supports transactions.

(Proof-of-Concept) Supports transforming [plumatic/schema](https://github.com/plumatic/schema) structures into draft 4 JSON [schemas](https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/#json-schema).

## Unsupported features

The [reactive streams API](https://www.mongodb.com/docs/languages/java/reactive-streams-driver/current/) is not supported.

The [legacy API](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-legacy/index.html) is not supported.

Deprecated constructs of the modern API (e.g., [MapReduce](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoCollection.html#mapReduce(com.mongodb.client.ClientSession,java.lang.String,java.lang.String))) are not supported.

## Some Design/Implementation Decisions

Use `:pre` conditions to check mandatory parameters.

Avoid lazyness. Use [IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java) as the root construct for queries. It gives us better control wrt. cleaning up cursors and offers better performance. Caller can use e.g. [eduction](https://github.com/clojure/clojure/blob/ce55092f2b2f5481d25cff6205470c1335760ef6/src/clj/clojure/core.clj#L7762) to slap more processing steps before reducing the result.

## TODO

- Strict / loose JSON schema construction (ie. whether to throw if schema can't be fully represented)
- Support for specifying JSON schema validation for a collection
- ...

## Examples

### Connecting

```clojure
(require '[murmeli.core :as m])

(def conn (-> {:uri       "mongodb://localhost:27017"
               :keywords? true}
              m/connect-client!
              (m/with-db "some-database")))

;; A random collection name for the examples below
(def coll :some-collection)
```

### Inserting documents

```clojure
(m/insert-many! conn coll [{:counter 1
                            :name    "bar"}
                           {:counter  2
                            :name     "quuz"
                            :location "somewhere"}
                           {:counter  3
                            :name     "asdf"
                            :location [123 456]}
                           {:name    "no counter here"
                            :aliases ["foo" "bar"]}
                           {:foo "bar"}])
```

### Query options

Query options can be supplied as keyword arguments

```clojure
(require '[murmeli.operators :refer [$exists $jsonSchema]])

(-> (m/find-all conn coll :query {:counter {$exists 1}} :projection [:name :counter] :limit 10)
    count)
;; => 3
```

or as a trailing map

```clojure
(-> (m/find-all conn coll {:query      {:counter {$exists 1}}
                           :projection [:name :counter]
                           :limit      10})
    count)
;; => 3
```

### Using `prismatic/schema` schemas for JSON schema validation / queries

```clojure
(require '[murmeli.validators.schema :as vs])
(require '[schema.core :as s :refer [defschema]])

(defschema MySchema
  {(s/optional-key :_id)      (s/pred #(instance? org.bson.types.ObjectId %))
   :name                      s/Str
   (s/optional-key :counter)  s/Int
   (s/optional-key :aliases)  #{s/Str}
   (s/optional-key :location) (s/cond-pre
                                s/Str
                                [s/Int])})

(def json-schema (vs/schema->json-schema MySchema))

(-> (m/find-all conn coll :query {$jsonSchema json-schema})
    count)
;; => 4
```

### Transactions

```clojure
(m/count-collection conn coll)
;; => 5

(m/with-session [conn (m/with-client-session-options conn {:read-preference :nearest})]
  (m/insert-one! conn coll {:name "foo"})
  (m/insert-one! conn coll {:name "quuz"}))

(m/count-collection conn coll)
;; => 7

(try
  (m/with-session [conn (m/with-client-session-options conn {:read-preference :nearest})]
    (m/insert-one! conn coll {:name "another"})
    (m/insert-one! conn coll {:name "one"})
    ;; Something goes wrong within `with-session`
    (throw (RuntimeException. "oh noes")))
  (catch Exception _
    ;; Rollback occurs
    nil))

(m/count-collection conn coll)
;; => 7
```

### Adding processing steps to the reducible

```clojure
(require '[schema.coerce :as sc])

(def coerce-my-record! (sc/coercer! MySchema sc/json-coercion-matcher))

(def by-schema (->> (m/find-reducible conn coll {:query {$jsonSchema json-schema}})
                    (eduction (map coerce-my-record!)
                              (filter (comp seq :aliases)))
                    (into [])))

(count by-schema)
;; => 1

(every? (comp set? :aliases) by-schema)
;; => true
```

### Cleanup

```clojure
(m/drop-db! conn "some-database")
```

## Development

### Linting

Initialize [clj-kondo](https://github.com/clj-kondo/clj-kondo) cache for the project:

```shell
bb run init-kondo!
```

Lint the project:

```shell
bb run lint
```

### Tests

Unit tests:

```shell
lein test
```

Check code examples in this `README.md`:

```shell
docker compose -f docker/docker-compose.yml up -d

lein run-doc-tests
```

## License

Copyright © 2024 Lasse Määttä

This program and the accompanying materials are made available under the
terms of the European Union Public License 1.2 which is available at
https://eupl.eu/1.2/en/
