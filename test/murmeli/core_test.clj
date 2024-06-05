(ns murmeli.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [murmeli.core :as m]
            [murmeli.test.utils :as test-utils]))

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(deftest simple-insert-test
  (testing "inserting document"
    (let [db-spec (test-utils/get-db-spec)]
      (is (string? (m/insert-one db-spec :coll {:foo 123})))
      (is (= 1 (m/count-collection db-spec :coll))))))


(deftest transaction-test
  (testing "exception in transaction"
    (let [coll    (keyword (str "coll-" (gensym)))
          db-spec (test-utils/get-db-spec)]
      (is (zero? (m/count-collection db-spec coll)))
      (try
        (m/with-session db-spec {}
          (m/insert-one db-spec coll {:foo 123})
          (throw (ex-info "foo" {})))
        (catch Exception e
          (is (= "foo" (.getMessage e)))))
      (is (zero? (m/count-collection db-spec coll))))))
