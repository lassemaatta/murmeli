(ns murmeli.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [murmeli.core :as m]
            [murmeli.test.utils :as test-utils]))

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(deftest simple-insert-test
  (testing "inserting document"
    (let [conn (test-utils/get-conn)]
      (is (string? (m/insert-one conn :coll {:foo 123})))
      (is (= 1 (m/count-collection conn :coll))))))


(deftest transaction-test
  (testing "exception in transaction"
    (let [coll (keyword (str "coll-" (gensym)))
          conn (test-utils/get-conn)]
      (is (zero? (m/count-collection conn coll)))
      (try
        (m/with-session conn {}
          (m/insert-one conn coll {:foo 123})
          (throw (ex-info "foo" {})))
        (catch Exception e
          (is (= "foo" (.getMessage e)))))
      (is (zero? (m/count-collection conn coll))))))
