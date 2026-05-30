(ns murmeli.data-interop-test
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.test.alpha :as stest]
            [clojure.spec.test.check :as-alias stc]
            [clojure.test :refer [deftest is]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as properties]
            [matcher-combinators.test]
            [murmeli.impl.data-interop :as di]
            [murmeli.specs :as ms]
            [murmeli.test.generators :as mg]))

(set! *warn-on-reflection* true)

(def passed [{:clojure.spec.test.check/ret {:pass?  true
                                            :result true}}])

(deftest read-concern->java-test
  (is (match? passed
              (stest/check `di/read-concern->java))))

(defspec read-concern-roundtrip-test
  (properties/for-all [rc (s/gen ::ms/read-concern)]
    (= (-> rc di/read-concern->java di/read-concern->clj)
       rc)))

(deftest write-concern->java-test
  (is (match? passed
              (stest/check `di/write-concern->java))))

(defspec write-concern-roundtrip-test
  (properties/for-all [wc (s/gen ::ms/write-concern)]
    (= (-> wc di/write-concern->java di/write-concern->clj)
       wc)))

(deftest read-preference->java-test
  (is (match? passed
              (stest/check `di/read-preference->java))))

(defspec read-preference-roundtrip-test
  (properties/for-all [rp (s/gen ::ms/read-preference)]
    (= (-> rp di/read-preference->java di/read-preference->clj)
       rp)))

(deftest make-client-settings-test
  (is (match? passed
              (stest/check `di/make-client-settings
                           {:gen {::ms/uri (constantly mg/uri-gen)}}))))

(deftest make-client-session-options-test
  (is (match? passed
              (stest/check `di/make-client-session-options))))

(deftest make-collation-test
  (is (match? passed
              (stest/check `di/make-collation))))

(deftest make-index-options-test
  (is (match? passed
              (stest/check `di/make-index-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 100
                            ::stc/opts {:num-tests 100}}))))

(deftest make-index-bson-test
  (is (match? passed
              (stest/check `di/make-index-bson))))

(deftest make-update-options-test
  (is (match? passed
              (stest/check `di/make-update-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 50}}))))

(deftest make-replace-options-test
  (is (match? passed
              (stest/check `di/make-replace-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 100
                            ::stc/opts {:num-tests 100}}))))

(deftest make-find-one-and-delete-options-test
  (is (match? passed
              (stest/check `di/make-find-one-and-delete-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 100
                            ::stc/opts {:num-tests 100}}))))

(deftest make-find-one-and-replace-options-test
  (is (match? passed
              (stest/check `di/make-find-one-and-replace-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 100
                            ::stc/opts {:num-tests 100}}))))

(deftest make-find-one-and-update-options-test
  (is (match? passed
              (stest/check `di/make-find-one-and-update-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 50}}))))

(deftest make-count-options-test
  (is (match? passed
              (stest/check `di/make-count-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 100}}))))

(deftest make-change-stream-options-test
  (is (match? passed
              (stest/check `di/make-change-stream-options))))

(deftest make-clustered-index-options-test
  (is (match? passed
              (stest/check `di/make-clustered-index-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 100}}))))

(deftest make-index-option-defaults-test
  (is (match? passed
              (stest/check `di/make-index-option-defaults
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 100}}))))

(deftest make-time-series-options-test
  (is (match? passed
              (stest/check `di/make-time-series-options))))

(deftest make-validation-options-test
  (is (match? passed
              (stest/check `di/make-validation-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 100}}))))

(deftest make-create-collection-options-test
  (is (match? passed
              (stest/check `di/make-create-collection-options
                           {:gen       {::ms/bson (constantly mg/bson-gen)}
                            ;; Generating the BSON can get expensive, so drop
                            ;; the number of test runs from 1k to 50
                            ::stc/opts {:num-tests 100}}))))

(deftest make-insert-one-options-test
  (is (match? passed
              (stest/check `di/make-insert-one-options))))

(deftest make-insert-many-options-test
  (is (match? passed
              (stest/check `di/make-insert-many-options))))

(deftest make-sort-test
  (is (match? passed
              (stest/check `di/make-sort))))
