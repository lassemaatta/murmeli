# Murmeli, a Clojure wrapper for the MongoDB Java driver

Wraps version 5.1 of the MongoDB Java [driver](https://www.mongodb.com/docs/drivers/java/sync/v5.1/).

Status: alpha

## Installation

Download from https://github.com/lassemaatta/murmeli.

## Examples

### Connecting

```clojure
(require '[murmeli.core :as m])

(def db-spec (-> {:uri      "mongodb://localhost:27017"
                  :database "some-database"}
                 m/connect-client!
                 m/with-db)) ;; or (m/with-db "another-database")
```

### Query options

```clojure
(require '[murmeli.operators :refer [$exists]])
;; query options as kw-args
(m/find-all db-spec :my-collection :query {:foo {$exists 1}} :projection [:foo :bar] :limit 10)

;; or as maps
(m/find-all db-spec :my-collection {:query      {:foo {$exists 1}}
                                    :projection [:foo :bar]
                                    :limit      10})
```

### Transactions

```clojure
(m/with-session [db-spec (m/with-client-session-options db-spec {:read-preference :nearest})]
  (m/insert-one db-spec :some-coll {:foo 123})
  (m/insert-one db-spec :other-coll {:bar 34}))
```

### Transforming documents eagerly

```clojure
(require '[schema.core :as s])
(require '[schema.coerce :as sc])

(defschema MyRecord
  {(s/optional-key :_id) s/Str
   :set                  #{s/Str}
   ...})

(def coerce-my-record! (sc/coercer! MyRecord sc/json-coercion-matcher))

(let [records (m/find-all db-spec :records :xform (map coerce-my-record!))]
  (assert (every? (comp set? :set) records)))
```

## License

Copyright © 2024 Lasse Määttä

This program and the accompanying materials are made available under the
terms of the European Union Public License 1.2 which is available at
https://eupl.eu/1.2/en/
