(ns murmeli.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [murmeli.core :as m]
            [murmeli.operators :refer [$lt]]
            [murmeli.test.utils :as test-utils]))

(set! *warn-on-reflection* true)

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(defn get-coll
  []
  (keyword (str "coll-" (gensym))))

(deftest simple-insert-test
  (testing "inserting document"
    (let [db-spec (test-utils/get-db-spec)
          coll    (get-coll)]
      (is (string? (m/insert-one! db-spec coll {:foo 123})))
      (is (= 1 (m/count-collection db-spec coll))))))

(deftest count-with-query-test
  (testing "count with query"
    (let [db-spec (test-utils/get-db-spec)
          coll    (get-coll)]
      (is (string? (m/insert-one! db-spec coll {:foo 123})))
      (is (= 1 (m/count-collection db-spec coll)))
      (is (= 0 (m/count-collection db-spec coll {:foo {$lt 100}})))
      (is (= 1 (m/count-collection db-spec coll {:foo {$lt 200}}))))))

(deftest transaction-test
  (testing "exception in transaction"
    (let [coll    (get-coll)
          db-spec (test-utils/get-db-spec)]
      (is (zero? (m/count-collection db-spec coll)))
      (try
        (m/with-session [db-spec (m/with-client-session-options db-spec {})]
          (m/insert-one! db-spec coll {:foo 123})
          (throw (ex-info "foo" {})))
        (catch Exception e
          (is (= "foo" (.getMessage e)))))
      (is (zero? (m/count-collection db-spec coll))))))

(deftest find-all-test
  (testing "find all"
    (let [coll    (get-coll)
          db-spec (test-utils/get-db-spec)
          id      (m/insert-one! db-spec coll {:foo 123})
          results (m/find-all db-spec coll)]
      (is (string? id))
      (is (= [{:_id id
               :foo 123}]
             results)))))
