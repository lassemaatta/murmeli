(ns murmeli.operators-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [murmeli.operators]))

(deftest operator-name-test
  (testing "each operator var name matches the value"
    (is (empty? (->> 'murmeli.operators
                     ns-publics
                     (filter (fn [[op-sym _op-var]] (str/starts-with? (name op-sym) "$")))
                     (filter (fn [[op-sym op-var]] (not= (name op-sym) (deref op-var))))
                     set)))))
