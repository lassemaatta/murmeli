(ns murmeli.convert-test
  (:require [clojure.test :refer [deftest is]]
            [murmeli.convert :as c])
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
