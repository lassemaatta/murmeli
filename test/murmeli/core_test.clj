(ns murmeli.core-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :as test :refer [deftest is testing]]
            [murmeli.core :as m]
            [murmeli.operators :refer [$exists $gt $jsonSchema $lt]]
            [murmeli.specs]
            [murmeli.test.utils :as test-utils]
            [murmeli.validators.schema :as vs]
            [schema.core :as s])
  (:import [com.mongodb MongoCommandException]))

(set! *warn-on-reflection* true)

(stest/instrument)

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(defn get-coll
  []
  (keyword (str "coll-" (gensym))))

(deftest db-test
  (testing "list databases "
    (let [db-spec (test-utils/get-db-spec)]
      (doseq [{:keys [name sizeOnDisk empty]} (m/list-dbs db-spec)]
        (is (string? name))
        (is (int? sizeOnDisk))
        (is (not empty))))))

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
      (is (= 1 (m/count-collection db-spec coll {:foo {$lt 200}})))
      (is (= 1 (m/estimated-count-collection db-spec coll))))))

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
      (is (zero? (m/count-collection db-spec coll)))

      (testing "happy path"
        (m/with-session [db-spec (m/with-client-session-options db-spec {})]
          (m/insert-one! db-spec coll {:foo 123})
          (is (= 1 (m/count-collection db-spec coll))))
        (is (= 1 (m/count-collection db-spec coll))))

      (testing "nested transactions"
        (m/with-session [db-spec (m/with-client-session-options db-spec {})]
          (m/with-session [db-spec db-spec]
            (m/insert-one! db-spec coll {:foo 123})
            (is (= 2 (m/count-collection db-spec coll))))
          (is (= 2 (m/count-collection db-spec coll)))
          (m/insert-one! db-spec coll {:foo 123}))
        (is (= 3 (m/count-collection db-spec coll)))))))

(deftest find-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        id      (m/insert-one! db-spec coll {:foo 123})
        id-2    (m/insert-one! db-spec coll {:bar "quuz"})
        item-1  {:_id id
                 :foo 123}
        item-2  {:_id id-2
                 :bar "quuz"}]
    (is (string? id))
    (is (string? id-2))
    (testing "find all"
      (let [results (m/find-all db-spec coll)]
        (is (= [item-1 item-2]
               results))))
    (testing "find all by query"
      (is (empty? (m/find-all db-spec coll :query {:foo {$gt 1000}})))
      (let [results (m/find-all db-spec coll :query {:foo {$gt 5}})]
        (is (= [item-1]
               results))))
    (testing "find one by query"
      (is (nil? (m/find-one db-spec coll :query {:foo {$gt 1000}})))
      (let [results (m/find-one db-spec coll :query {:foo {$gt 5}})]
        (is (= item-1 results)))
      (testing "find-one throws on multiple hits"
        (is (thrown-with-msg? RuntimeException
                              #"find-one found multiple results"
                              (m/find-one db-spec coll :query {:_id {$exists 1}})))))))

(deftest index-test
  (let [coll     (get-coll)
        db-spec  (test-utils/get-db-spec)
        id-index {:name "_id_"
                  :key  {:_id 1}
                  :v    2}]
    (m/insert-one! db-spec coll {:foo 123})
    (testing "initially no indexes"
      (is (= [id-index]
             (m/list-indexes db-spec coll))))
    (testing "creating indexes"
      (m/create-index! db-spec coll {:foo 1} {:name    "my-index"
                                              :unique? true
                                              :sparse? true})
      (m/create-index! db-spec coll {:foo.bar 1} {:name "my-index-2"})
      (m/create-index! db-spec coll {:foo.quuz 1} {:name "my-index-3"})
      (is (= [id-index
              {:name   "my-index"
               :key    {:foo 1}
               :unique true
               :sparse true
               :v      2}
              {:name "my-index-2"
               :key  {:foo.bar 1}
               :v    2}
              {:name "my-index-3"
               :key  {:foo.quuz 1}
               :v    2}]
             (m/list-indexes db-spec coll))))
    (testing "removing index"
      (testing "removing with wrong name fails"
        (is (thrown-with-msg? MongoCommandException
                              #"Command failed with error 27 \(IndexNotFound\).*"
                              (m/drop-index-by-name! db-spec coll "missing"))))
      (testing "removing with correct name works"
        (m/drop-index-by-name! db-spec coll "my-index")
        (is (= [id-index
                {:name "my-index-2"
                 :key  {:foo.bar 1}
                 :v    2}
                {:name "my-index-3"
                 :key  {:foo.quuz 1}
                 :v    2}]
               (m/list-indexes db-spec coll))))
      (testing "removing with keys works"
        (m/drop-index! db-spec coll {:foo.bar 1})
        (is (= [id-index
                {:name "my-index-3"
                 :key  {:foo.quuz 1}
                 :v    2}]
               (m/list-indexes db-spec coll))))
      (testing "removing all indexes works"
        (m/drop-all-indexes! db-spec coll)
        (is (= [id-index]
               (m/list-indexes db-spec coll)))))))

(deftest schema-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        id-0    (m/insert-one! db-spec coll {})
        id-1    (m/insert-one! db-spec coll {:foo "just foo"})
        id-2    (m/insert-one! db-spec coll {:foo "foo and a timestamp"
                                             :bar #inst "2024-06-01"})
        id-3    (m/insert-one! db-spec coll {:bar #inst "2024-06-02"})]

    (testing "one required field"
      (let [schema (vs/schema->json-schema
                     {:foo s/Str})]
        (is (= {:_id id-1
                :foo "just foo"}
               (m/find-one db-spec coll :query {$jsonSchema schema})))))

    (testing "two required fields"
      (let [schema (vs/schema->json-schema
                     {:foo s/Str
                      :bar s/Inst})]
        (is (= {:_id id-2
                :foo "foo and a timestamp"
                :bar #inst "2024-06-01"}
               (m/find-one db-spec coll :query {$jsonSchema schema})))))

    (testing "one optional, one required"
      (let [schema (vs/schema->json-schema
                     {(s/optional-key :foo) s/Str
                      :bar                  s/Inst})]
        (is (= [{:_id id-2
                 :foo "foo and a timestamp"
                 :bar #inst "2024-06-01"}
                {:_id id-3
                 :bar #inst "2024-06-02"}]
               (m/find-all db-spec coll :query {$jsonSchema schema})))))

    (testing "open schema"
      (let [schema (vs/schema->json-schema
                     {s/Keyword s/Any})]
        (is (= [{:_id id-0}
                {:_id id-1
                 :foo "just foo"}
                {:_id id-2
                 :foo "foo and a timestamp"
                 :bar #inst "2024-06-01"}
                {:_id id-3
                 :bar #inst "2024-06-02"}]
               (m/find-all db-spec coll :query {$jsonSchema schema})))))))
