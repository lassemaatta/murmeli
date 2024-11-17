(ns murmeli.data-interop-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.spec.test.check :as-alias stc]
            [clojure.test :refer [deftest is]]
            [matcher-combinators.test]
            [murmeli.data-interop :as di]
            [murmeli.specs :as ms]
            [murmeli.test.generators :as mg]))

(set! *warn-on-reflection* true)

(def passed [{:clojure.spec.test.check/ret {:pass?  true
                                            :result true}}])

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
