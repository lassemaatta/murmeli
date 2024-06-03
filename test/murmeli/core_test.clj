(ns murmeli.core-test
  (:require [clojure.test :as test :refer [deftest is testing]]
            [murmeli.core :as m]
            [murmeli.test.utils :as test-utils]))

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(deftest a-test
  (testing "FIXME, I fail."
    (let [conn (test-utils/get-conn)]
      (is (string? (m/insert-one conn :coll {:foo 123})))
      (is (= 1 (m/count-collection conn :coll))))))
