(ns murmeli.convert-test
  (:require [clojure.test :refer [are deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [murmeli.convert :as c]
            [murmeli.specs])
  (:import [org.bson BsonArray
                     BsonBoolean
                     BsonDateTime
                     BsonDecimal128
                     BsonDocument
                     BsonDouble
                     BsonInt32
                     BsonInt64
                     BsonNull
                     BsonString]))

(deftest to-bson-test
  (is (instance? BsonNull (c/to-bson nil)))
  (is (instance? BsonBoolean (c/to-bson true)))
  (is (instance? BsonInt32 (c/to-bson (int 1))))
  (is (instance? BsonInt64 (c/to-bson (long 1))))
  (is (instance? BsonDouble (c/to-bson (double 1.234))))
  (is (instance? BsonDecimal128 (c/to-bson (bigdec 1.234))))
  (is (instance? BsonDecimal128 (c/to-bson (bigint 1234))))
  (is (instance? BsonDouble (c/to-bson (/ 1 3))))
  (is (instance? BsonDateTime (c/to-bson #inst "2024-06-01")))
  (is (instance? BsonString (c/to-bson "hello")))
  (is (instance? BsonString (c/to-bson :foo)))
  (is (instance? BsonString (c/to-bson :bar/foo)))
  (is (instance? BsonString (c/to-bson 'foo)))
  (is (instance? BsonString (c/to-bson 'bar/foo)))
  (is (instance? BsonArray (c/to-bson (list 1 2 3))))
  (is (instance? BsonArray (c/to-bson (range 0 10))))
  (is (instance? BsonArray (c/to-bson [true (int 1) (long 2) "hello" nil])))
  (is (instance? BsonArray (c/to-bson #{true (int 1) (long 2) "hello" nil})))
  (is (instance? BsonDocument (c/to-bson {"key" "value"})))
  (is (instance? BsonDocument (c/to-bson {:foo  1
                                          :bar  [1 2 3]
                                          :quuz #{"hello"}}))))

(deftest invalid-keys-test
  (is (thrown-with-msg? RuntimeException
                        #"Not a valid BSON map key"
                        (c/to-bson {[1] :a-vector}))))

(def roundtrip (comp c/from-bson c/to-bson))

(deftest simple-roundtrip-test
  (are [value] (= value (roundtrip value))
    nil
    true
    false
    "a string"
    (long 123)
    Long/MIN_VALUE
    Long/MAX_VALUE
    (int 123)
    (int Integer/MIN_VALUE)
    (int Integer/MAX_VALUE)
    (double 1.23)
    (double Double/MIN_VALUE)
    (double Double/MAX_VALUE)
    (bigdec 1.23)
    (bigdec Long/MAX_VALUE)
    (bigdec Long/MIN_VALUE)
    #inst "0000-01-01"
    #inst "2024-06-01"
    #inst "9999-12-31"
    (list 1 2 3)
    [1 2 3]
    {"a" "v"
     "b" [1 2 3]
     "c" 1
     "d" {"a" 1}}))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(defspec to-bson-props 100
  (prop/for-all [v gen/any]
    (try
      (c/to-bson v)
      (catch Exception e
        (let [msg (.getMessage e)]
          (when-not (or (re-matches #"Conversion to Decimal128 would require inexact rounding of.*" msg)
                        (re-matches #"Not a valid BSON map key.*" msg))
            (throw e))
          true)))))

#_{:clj-kondo/ignore [:unresolved-symbol]}
(defspec map->bson-props 100
  (prop/for-all [m (gen/map (gen/one-of
                              [gen/string-ascii
                               gen/symbol
                               gen/keyword])
                            gen/any)]
    (c/map->bson m)))
