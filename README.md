# Murmeli, a Clojure wrapper for the MongoDB Java driver

Wraps version 5.1 of the MongoDB Java [driver](https://www.mongodb.com/docs/drivers/java/sync/v5.1/).

Status: alpha

## Installation

Download from https://github.com/lassemaatta/murmeli.

## TODO

- Various connection options (SSL etc)
- Update documents functionality
- Strict / loose JSON schema construction (ie. whether to throw if schema can't be fully represented)
- Support for specifying JSON schema validation for a collection
- ...

## Examples

### Connecting

```clojure
(require '[murmeli.core :as m])

(def db-spec (-> {:uri           "mongodb://localhost:27017"
                  :database-name "some-database"}
                 m/connect-client!
                 m/with-db)) ;; or (m/with-db "another-database")

;; A random collection name for the examples below
(def coll :some-collection)
```

### Inserting documents

```clojure
(m/insert-many! db-spec coll [{:counter 1
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

(-> (m/find-all db-spec coll :query {:counter {$exists 1}} :projection [:name :counter] :limit 10)
    count)
;; => 3
```

or as a trailing map

```clojure
(-> (m/find-all db-spec coll {:query      {:counter {$exists 1}}
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
  {(s/optional-key :_id)      s/Str
   :name                      s/Str
   (s/optional-key :counter)  s/Int
   (s/optional-key :aliases)  #{s/Str}
   (s/optional-key :location) (s/cond-pre
                                s/Str
                                [s/Int])})

(def json-schema (vs/schema->json-schema MySchema))

(-> (m/find-all db-spec coll :query {$jsonSchema json-schema})
    count)
;; => 4
```

### Transactions

```clojure
(m/count-collection db-spec coll)
;; => 5

(m/with-session [db-spec (m/with-client-session-options db-spec {:read-preference :nearest})]
  (m/insert-one! db-spec coll {:name "foo"})
  (m/insert-one! db-spec coll {:name "quuz"}))

(m/count-collection db-spec coll)
;; => 7

(try
  (m/with-session [db-spec (m/with-client-session-options db-spec {:read-preference :nearest})]
    (m/insert-one! db-spec coll {:name "another"})
    (m/insert-one! db-spec coll {:name "one"})
    ;; Something goes wrong within `with-session`
    (throw (RuntimeException. "oh noes")))
  (catch Exception _
    ;; Rollback occurs
    nil))

(m/count-collection db-spec coll)
;; => 7
```

### Transforming documents eagerly

```clojure
(require '[schema.coerce :as sc])

(def coerce-my-record! (sc/coercer! MySchema sc/json-coercion-matcher))

(def by-schema (m/find-all db-spec coll {:query {$jsonSchema json-schema}
                                         :xform (comp (map coerce-my-record!)
                                                      (filter (comp seq :aliases)))}))

(count by-schema)
;; => 1

(every? (comp set? :aliases) by-schema)
;; => true
```

### Cleanup

```clojure
(m/drop-db! db-spec "some-database")
```

## License

Copyright © 2024 Lasse Määttä

This program and the accompanying materials are made available under the
terms of the European Union Public License 1.2 which is available at
https://eupl.eu/1.2/en/
