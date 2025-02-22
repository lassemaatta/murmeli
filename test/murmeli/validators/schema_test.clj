(ns murmeli.validators.schema-test
  (:require [clojure.test :refer [deftest is testing]]
            [murmeli.validators.schema :as validator]
            [schema.core :as s :refer [defschema]]))

(deftest scalars-test
  (let [schema (validator/schema->json-schema
                 {:a s/Any
                  :b s/Bool
                  :c s/Inst
                  :d s/Int
                  :e s/Keyword
                  :f s/Num
                  :g s/Regex
                  :h s/Str
                  :i s/Symbol
                  :j s/Uuid})]
    (is (= {:bsonType             :object
            :required             ["_id" "a" "b" "c" "d" "e" "f" "g" "h" "i" "j"]
            :properties           {"_id" {:bsonType :objectId}
                                   "a"   {}
                                   "b"   {:bsonType :bool}
                                   "c"   {:bsonType :date}
                                   "d"   {:bsonType :long}
                                   "e"   {:bsonType :string}
                                   "f"   {:bsonType :number}
                                   "g"   {:bsonType :string}
                                   "h"   {:bsonType :string}
                                   "i"   {:bsonType :string}
                                   "j"   {:bsonType :string
                                          :format   :uuid}}
            :additionalProperties false}
           schema))))

(deftest id-test
  (testing "assign ObjectId _id by default"
    (is (=  {:bsonType             :object
             :additionalProperties false
             :required             ["_id"]
             :properties           {"_id" {:bsonType :objectId}}}
            (validator/schema->json-schema
              {}))))
  (testing "schema may specify an alternate type for _id"
    (is (=  {:bsonType             :object
             :additionalProperties false
             :required             ["_id"]
             :properties           {"_id" {:bsonType :string}}}
            (validator/schema->json-schema
              {:_id s/Str})))))

(deftest collection-test
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType :array
                                        :items    {:bsonType :string}}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a [s/Str]})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType    :array
                                        :uniqueItems true
                                        :items       [{:bsonType :string}]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a #{s/Str}})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType             :object
                                        :additionalProperties true}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a {s/Int s/Keyword}}))))

(deftest enum-test
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:enum [1 2 3 "a" "b" "c" "xyz"]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/enum 1 2 3 "a" "b" "c" :a :b :xyz)}))))

(deftest eq-test
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:enum [1337]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/eq 1337)}))))

(deftest optional-keys-test
  (is (= {:bsonType             :object
          :required             ["_id" "a" "b"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType :string}
                                 "b"   {:bsonType :string}
                                 "c"   {:bsonType :string}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a                  s/Str
            (s/required-key :b) s/Str
            (s/optional-key :c) s/Str}))))

(defschema Nested
  {:x s/Str
   :y [s/Str]
   :z {:o s/Str}})

(defschema Outer
  {:a Nested
   :b [s/Str]})

(deftest nested-test
  (let [schema (validator/schema->json-schema Outer)]
    (is (= {:bsonType             :object
            :required             ["_id" "a" "b"]
            :properties           {"_id" {:bsonType :objectId}
                                   "a"   {:bsonType             :object
                                          :required             ["x" "y" "z"]
                                          :properties           {"x" {:bsonType :string}
                                                                 "y" {:bsonType :array
                                                                      :items    {:bsonType :string}}
                                                                 "z" {:bsonType             :object
                                                                      :required             ["o"]
                                                                      :properties           {"o" {:bsonType :string}}
                                                                      :additionalProperties false}}
                                          :additionalProperties false}
                                   "b"   {:bsonType :array
                                          :items    {:bsonType :string}}}
            :additionalProperties false}
           schema))))

(deftest wrapper-test
  (is (thrown-with-msg? RuntimeException
                        #"Cannot represent arbitrary predicates as JSON schema"
                        (validator/schema->json-schema
                          {:a (s/pred some?)})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType [:null :string]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/maybe s/Str)})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType    :string
                                        :description "I'm a string"}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/named s/Str "I'm a string")})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType :string}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/constrained s/Str string?)})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:bsonType    :string
                                        :description "should be string"}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/constrained s/Str string? "should be string")})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:anyOf [{:bsonType :array
                                                 :items    {:bsonType :string}}
                                                {:bsonType             :object
                                                 :additionalProperties true}
                                                {:bsonType :string}
                                                {:bsonType :long}
                                                {:bsonType :string
                                                 :format   :uuid}]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/cond-pre
                 [s/Str]
                 {s/Int s/Keyword}
                 s/Str
                 s/Int
                 s/Uuid)})))
  (is (= {:bsonType             :object
          :required             ["_id" "a"]
          :properties           {"_id" {:bsonType :objectId}
                                 "a"   {:anyOf [{:bsonType :array
                                                 :items    {:bsonType :string}}
                                                {:bsonType             :object
                                                 :additionalProperties true}
                                                {:bsonType :string}
                                                {:bsonType :long}
                                                {:bsonType :string
                                                 :format   :uuid}]}}
          :additionalProperties false}
         (validator/schema->json-schema
           {:a (s/conditional
                 :foo [s/Str]
                 :bar {s/Int s/Keyword}
                 :baz s/Str
                 :quuz s/Int
                 :flor s/Uuid)})))
  (is (= {:bsonType             :object
          :additionalProperties false
          :required             ["_id" "a"]
          :properties           {"a"   {:allOf [{:bsonType :string}
                                                {:bsonType :string
                                                 :format   :uuid}]}
                                 "_id" {:bsonType :objectId}}}
         (validator/schema->json-schema
           {:a (s/both s/Str s/Uuid)}))))
