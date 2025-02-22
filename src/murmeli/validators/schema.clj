(ns murmeli.validators.schema
  "Draft 4 JSON schema support for plumatic/schema.

  Quite experimental and subject to change.

  Note that the JSON schema which MongoDB supports is not identical
  to the standard Draft 4 specification:
  https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/#json-schema"
  (:require [schema.core :as s])
  (:import [clojure.lang Cons
                         IPersistentList
                         IPersistentMap
                         IPersistentSet
                         IPersistentVector
                         Keyword
                         Symbol]))

(set! *warn-on-reflection* true)

(def ObjectId
  "ObjectId instance"
  (s/pred (fn [o] (instance? org.bson.types.ObjectId o)) 'objectId?))

(declare to-schema)

(defprotocol ToJsonKey
  (-to-key [this]))

(extend-protocol ToJsonKey
  String
  (-to-key [this]
    this)
  ;; Don't produce a property key if a map contains an anonymous key like `Str`,
  ;; e.g. `{s/Str s/Int}`
  Symbol
  (-to-key [_]
    nil)
  Keyword
  (-to-key [this]
    (name this))
  IPersistentList
  (-to-key [this]
    (-> this second name)))

(defprotocol ToJsonSchema
  (-to-schema [this opts]))

(extend-protocol ToJsonSchema
  Symbol
  (-to-schema [this {:keys [null?
                            description]}]
    (cond-> (case this
              Any      {}
              ObjectId {:bsonType :objectId}
              Bool     {:bsonType :bool}
              Int      {:bsonType :long}
              Num      {:bsonType :number}
              Str      {:bsonType :string}
              Keyword  {:bsonType :string}
              Symbol   {:bsonType :string}
              Regex    {:bsonType :string}
              Uuid     {:bsonType :string
                        :format   :uuid}
              Inst     {:bsonType :date}
              {:bsonType :symbol})
      ;; Are we wrapped in a `s/maybe`?
      null?       (update :bsonType (fn [other]
                                      [:null other]))
      description (assoc :description description)))
  ;; No idea why some schema sequences (enum, cond-pre) are wrapped in Cons and others
  ;; in IPersistenLists (maybe, named, ..)
  Cons
  (-to-schema [this _]
    (case (first this)
      enum        {:enum (->> (rest this)
                              (map (fn [x]
                                     (if (keyword? x)
                                       (name x)
                                       x)))
                              distinct
                              (sort-by str)
                              (into []))}
      ;; cond-pre lists a bunch of disjoint schemas, we must match one of them
      cond-pre    {:anyOf (->> (rest this)
                               (mapv to-schema))}
      both        {:allOf (->> (rest this)
                               (mapv to-schema))}
      ;; conditional has pairs of predicate + schema. We can't really represent the
      ;; predicates, but at least we can try to check if one of the schemas matches
      conditional {:anyOf (->> (rest this)
                               (partition 2)
                               (mapv (comp to-schema second)))}))
  IPersistentVector
  (-to-schema [this _]
    {:bsonType :array
     :items    (to-schema (first this))})
  IPersistentSet
  (-to-schema [this _]
    {:bsonType    :array
     :uniqueItems true
     :items       (mapv to-schema this)})
  IPersistentList
  (-to-schema [[wrapper-type schema & args] {:keys [id?]}]
    (if id?
      {:bsonType :objectId}
      (case wrapper-type
        eq          {:enum [schema]}
        maybe       (to-schema schema {:null? true})
        named       (to-schema schema {:description (first args)})
        constrained (to-schema schema {:description (when (string? (first args))
                                                      (first args))})
        pred        (case schema
                      objectId? {:bsonType :objectId}
                      (throw (ex-info "Cannot represent arbitrary predicates as JSON schema"
                                      {:pred schema}))))))
  IPersistentMap
  (-to-schema [this _]
    (let [required    (some->> (keys this)
                               (remove list?)
                               (keep -to-key)
                               sort
                               (into []))
          ;; Find all named properties
          props       (some->> this
                               (keep (fn [[k v]]
                                       (when-let [k (-to-key k)]
                                         [k (to-schema v)])))
                               (into {}))
          ;; Contains open keys like `s/Str`?
          additional? (boolean (some->> (keys this)
                                        (filter symbol?)
                                        seq))]
      (merge {:bsonType             :object
              :additionalProperties additional?}
             (when (seq required)
               {:required required})
             (when (seq props)
               {:properties props})
             (when additional?
               {})))))

(defn- to-schema
  ([schema]
   (to-schema schema {}))
  ([schema opts]
   (-to-schema schema opts)))

(defn schema->json-schema
  [schema]
  (-> schema
      ;; Make sure the root document mentions `_id`, otherwise
      ;; we won't match any documents (unless `additionalProperties` is true)
      (update :_id (fnil identity ObjectId))
      ;; Expand schema to make it easier to parse
      s/explain
      to-schema))
