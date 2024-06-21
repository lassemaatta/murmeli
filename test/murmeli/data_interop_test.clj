(ns murmeli.data-interop-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.generators :as gen]
            [matcher-combinators.test]
            [murmeli.data-interop :as di]
            [murmeli.specs :as ms]))

(def passed [{:clojure.spec.test.check/ret {:pass?  true
                                            :result true}}])

(def uri-gen #(gen/return "mongodb://localhost:27017"))

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
  (is (match? passed
              (stest/check `di/make-index-options))))

(deftest make-index-bson-test
  (is (match? passed
              (stest/check `di/make-index-bson))))
