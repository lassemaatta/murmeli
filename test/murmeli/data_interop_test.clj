(ns murmeli.data-interop-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [matcher-combinators.test]
            [murmeli.convert :as mc]
            [murmeli.data-interop :as di]
            [murmeli.specs :as ms])
  (:import [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

(def passed [{:clojure.spec.test.check/ret {:pass?  true
                                            :result true}}])

(def uri-gen #(gen/return "mongodb://localhost:27017"))

(def object-id-gen #(gen/let [date gen/nat
                              counter gen/nat]
                      (ObjectId. (int date) (int counter))))

(def regex-gen #(gen/return #"foo"))

(def registry (mc/registry {:keywords? true}))

(def bson-gen #(gen/let [doc (s/gen ::ms/document {::ms/object-id object-id-gen
                                                   ::ms/regex     regex-gen})]
                 (mc/map->bson doc registry)))

;(println (gen/sample (bson-gen) 1))

(deftest get-read-concern-test
  (is (match? passed
              (stest/check `di/get-read-concern))))

(deftest get-write-concern-test
  (is (match? passed
              (stest/check `di/get-write-concern))))

(deftest get-read-preference-test
  (is (match? passed
              (stest/check `di/get-read-preference))))

(deftest make-client-settings-test
  (is (match? passed
              (stest/check `di/make-client-settings
                           {:gen {::ms/uri uri-gen}}))))

(deftest make-client-session-options-test
  (is (match? passed
              (stest/check `di/make-client-session-options))))

(deftest make-index-options-test
  (binding [s/*recursion-limit* 1]
    (is (match? passed
                (stest/check `di/make-index-options
                             {:gen {::ms/bson bson-gen}})))))

(deftest make-index-bson-test
  (is (match? passed
              (stest/check `di/make-index-bson))))

(deftest make-update-options-test
  (is (match? passed
              (stest/check `di/make-update-options))))
