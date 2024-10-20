(ns murmeli.core-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :as test :refer [deftest is testing]]
            [matcher-combinators.test]
            [murmeli.core :as m]
            [murmeli.operators :refer [$exists $gt $jsonSchema $lt $set]]
            [murmeli.specs]
            [murmeli.test.utils :as test-utils]
            [murmeli.validators.schema :as vs]
            [schema.coerce :as sc]
            [schema.core :as s :refer [defschema]])
  (:import [com.mongodb MongoCommandException]))

(set! *warn-on-reflection* true)

(stest/instrument)

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(defn get-coll
  "Return a random unique collection name for a test"
  []
  (keyword (str "coll-" (gensym))))

(deftest id-test
  (is (not (m/id? nil)))
  (is (not (m/id? "")))
  (is (m/id? "667c471cea82561061cb1a96"))
  (is (m/id? (m/create-id))))

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
          coll    (get-coll)
          id      (m/insert-one! db-spec coll {:foo 123})]
      (is (m/id? id))
      (is (= 1 (m/count-collection db-spec coll)))
      (is (= {:_id id
              :foo 123}
             (m/find-one db-spec coll :query {:_id id}))))))

(deftest simple-insert-many-test
  (testing "inserting documents"
    (let [db-spec                  (test-utils/get-db-spec)
          coll                     (get-coll)
          [id-1 id-2 id-3 :as ids] (m/insert-many! db-spec coll [{:foo 1}
                                                                 {:foo 2}
                                                                 {:foo 3}])]
      (is (every? m/id? ids))
      (is (= 3 (count ids)))
      (is (= 3 (m/count-collection db-spec coll)))
      (is (= [{:_id id-1 :foo 1}
              {:_id id-2 :foo 2}
              {:_id id-3 :foo 3}] (m/find-all db-spec coll)))
      (is (= {:_id id-1 :foo 1} (m/find-one db-spec coll :query {:_id id-1})))
      (is (= {:_id id-2 :foo 2} (m/find-one db-spec coll :query {:_id id-2})))
      (is (= {:_id id-3 :foo 3} (m/find-one db-spec coll :query {:_id id-3}))))))

(deftest count-with-query-test
  (testing "count with query"
    (let [db-spec (test-utils/get-db-spec)
          coll    (get-coll)]
      (is (m/id? (m/insert-one! db-spec coll {:foo 123})))
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
        (m/with-session [db-spec (m/with-client-session-options db-spec)]
          (m/insert-one! db-spec coll {:foo 123})
          (throw (ex-info "foo" {})))
        (catch Exception e
          (is (= "foo" (.getMessage e)))))
      (is (zero? (m/count-collection db-spec coll)))

      (testing "happy path"
        (m/with-session [db-spec (m/with-client-session-options db-spec)]
          (m/insert-one! db-spec coll {:foo 123})
          (is (= 1 (m/count-collection db-spec coll))))
        (is (= 1 (m/count-collection db-spec coll))))

      (testing "nested transactions"
        (m/with-session [db-spec (m/with-client-session-options db-spec)]
          (m/with-session [db-spec db-spec]
            (m/insert-one! db-spec coll {:foo 123})
            (is (= 2 (m/count-collection db-spec coll))))
          (is (= 2 (m/count-collection db-spec coll)))
          (m/insert-one! db-spec coll {:foo 123}))
        (is (= 3 (m/count-collection db-spec coll)))))))

(deftest update-one-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        [id-1]  (m/insert-many! db-spec coll [{:foo 1}])]
    (testing "update single document"
      (is (= {:modified 1
              :matched  1}
             (m/update-one! db-spec coll {:_id id-1} [{$set {:foo 10}}]))))
    (testing "same update again -> no changes"
      (is (= {:modified 0
              :matched  1}
             (m/update-one! db-spec coll {:_id id-1} [{$set {:foo 10}}]))))
    (testing "no match"
      (is (= {:modified 0
              :matched  0}
             (m/update-one! db-spec coll {:quuz "kukka"} [{$set {:foo 10}}]))))))

(deftest update-many-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        [id-1]  (m/insert-many! db-spec coll [{:foo 1}
                                              {:foo 2}
                                              {:foo 3}])]
    (testing "update single document"
      (is (= {:modified 1
              :matched  1}
             (m/update-many! db-spec coll {:_id id-1} [{$set {:foo 10}}]))))
    (testing "same update again -> no changes"
      (is (= {:modified 0
              :matched  1}
             (m/update-many! db-spec coll {:_id id-1} [{$set {:foo 10}}]))))
    (testing "no match"
      (is (= {:modified 0
              :matched  0}
             (m/update-many! db-spec coll {:quuz "kukka"} [{$set {:foo 10}}]))))
    (testing "update multiple documents"
      (is (= {:modified 2 ; id-1 already has :foo as 10
              :matched  3}
             (m/update-many! db-spec coll {:foo {$exists true}} [{$set {:foo 10}}]))))
    (testing "update multiple fields"
      (is (= {:modified 3
              :matched  3}
             (m/update-many! db-spec coll {:foo {$exists true}} [{$set {:bar 1}}
                                                                 {$set {:quuz 1}}]))))))

(deftest find-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        id      (m/insert-one! db-spec coll {:foo 123})
        id-2    (m/insert-one! db-spec coll {:bar "quuz"})
        id-3    (m/insert-one! db-spec coll {:foo 200
                                             :bar "aaaa"})
        item-1  {:_id id
                 :foo 123}
        item-2  {:_id id-2
                 :bar "quuz"}
        item-3  {:_id id-3
                 :foo 200
                 :bar "aaaa"}]
    (is (m/id? id))
    (is (m/id? id-2))
    (is (m/id? id-3))
    (testing "find all"
      (let [results (m/find-all db-spec coll)]
        (is (= [item-1 item-2 item-3]
               results)))
      (testing "projection"
        (let [results (m/find-all db-spec coll :projection [:_id])]
          (is (= [{:_id id} {:_id id-2} {:_id id-3}]
                 results)))
        (let [results (m/find-all db-spec coll :projection [:foo])]
          (is (= [{:_id id :foo 123} {:_id id-2} {:_id id-3 :foo 200}]
                 results))))
      (testing "sorting"
        (let [results (m/find-all db-spec coll :sort (array-map :foo 1))]
          (is (= [item-2 item-1 item-3]
                 results)))
        (let [results (m/find-all db-spec coll :sort (array-map :foo -1))]
          (is (= [item-3 item-1 item-2]
                 results)))))
    (testing "find all by query"
      (is (empty? (m/find-all db-spec coll :query {:foo {$gt 1000}})))
      (let [results (m/find-all db-spec coll :query {:foo {$gt 5
                                                           $lt 150}})]
        (is (= [item-1]
               results))))
    (testing "find one by query"
      (is (nil? (m/find-one db-spec coll :query {:foo {$gt 1000}})))
      (let [results (m/find-one db-spec coll :query {:foo {$gt 5
                                                           $lt 150}})]
        (is (= item-1 results)))
      (testing "find-one throws on multiple hits"
        (is (thrown-with-msg? RuntimeException
                              #"find-one found multiple results"
                              (m/find-one db-spec coll :query {:_id {$exists 1}})))))))

(deftest str-coll-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        id      (m/insert-one! db-spec coll {"foo" 123})
        id-2    (m/insert-one! db-spec coll {"bar" "quuz"})
        id-3    (m/insert-one! db-spec coll {"foo" 200
                                             "bar" "aaaa"})
        item-1  {"_id" id
                 "foo" 123}
        item-2  {"_id" id-2
                 "bar" "quuz"}
        item-3  {"_id" id-3
                 "foo" 200
                 "bar" "aaaa"}]
    (testing "find all"
      (let [results (m/find-all db-spec coll :keywords? false)]
        (is (= [item-1 item-2 item-3]
               results)))
      (testing "projection"
        (let [results (m/find-all db-spec coll :projection ["_id"] :keywords? false)]
          (is (= [{"_id" id} {"_id" id-2} {"_id" id-3}]
                 results)))
        (let [results (m/find-all db-spec coll :projection ["foo"] :keywords? false)]
          (is (= [{"_id" id "foo" 123} {"_id" id-2} {"_id" id-3 "foo" 200}]
                 results))))
      (testing "sorting"
        (let [results (m/find-all db-spec coll :sort (array-map "foo" 1) :keywords? false)]
          (is (= [item-2 item-1 item-3]
                 results)))
        (let [results (m/find-all db-spec coll :sort (array-map "foo" -1) :keywords? false)]
          (is (= [item-3 item-1 item-2]
                 results)))))
    (testing "find all by query"
      (is (empty? (m/find-all db-spec coll :query {"foo" {$gt 1000}} :keywords? false)))
      (let [results (m/find-all db-spec coll :query {"foo" {$gt 5
                                                            $lt 150}} :keywords? false)]
        (is (= [item-1]
               results))))
    (testing "find one by query"
      (is (nil? (m/find-one db-spec coll :query {:foo {$gt 1000}} :keywords? false)))
      (let [results (m/find-one db-spec coll :query {:foo {$gt 5
                                                           $lt 150}} :keywords? false)]
        (is (= item-1 results)))
      (testing "find-one throws on multiple hits"
        (is (thrown-with-msg? RuntimeException
                              #"find-one found multiple results"
                              (m/find-one db-spec coll :query {"_id" {$exists 1}} :keywords? false)))))))

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
      (m/create-index! db-spec coll {:foo.quuz 1
                                     :foo.quiz 1} {:name "my-index-3"})
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
               :key  {:foo.quuz 1
                      :foo.quiz 1}
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
                 :key  {:foo.quuz 1
                        :foo.quiz 1}
                 :v    2}]
               (m/list-indexes db-spec coll))))
      (testing "removing with keys works"
        (m/drop-index! db-spec coll {:foo.bar 1})
        (is (= [id-index
                {:name "my-index-3"
                 :key  {:foo.quuz 1
                        :foo.quiz 1}
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

(defschema MyRecord
  {(s/optional-key :_id) s/Str
   :set                  #{s/Str}
   :vec                  [s/Int]
   :map                  {s/Keyword s/Keyword}})

(def valid-my-record! (s/validator MyRecord))
(def coerce-my-record! (sc/coercer! MyRecord sc/json-coercion-matcher))

(deftest external-xform-test
  (let [coll    (get-coll)
        db-spec (test-utils/get-db-spec)
        input   [{:set #{}
                  :vec []
                  :map {}}
                 {:set #{"a" "b" "c"}
                  :vec [1 2 3]
                  :map {:a :x
                        :b :y}}]]
    (is (nil? (run! valid-my-record! input)))
    (is (= 2 (count (m/insert-many! db-spec coll input))))
    (testing "read out as-is"
      (let [out-plain (m/find-all db-spec coll)]
        (is (match? [{:_id m/id?
                      :set []
                      :vec []
                      :map {}}
                     {:_id m/id?
                      :set ["a" "b" "c"]
                      :vec [1 2 3]
                      :map {:a "x"
                            :b "y"}}]
                    out-plain))))
    (testing "coerce using xform"
      (let [out-plain (m/find-all db-spec coll :xform (map coerce-my-record!))]
        (is (match? [{:_id m/id?
                      :set #{}
                      :vec []
                      :map {}}
                     {:_id m/id?
                      :set #{"a" "b" "c"}
                      :vec [1 2 3]
                      :map {:a :x
                            :b :y}}]
                    out-plain))))
    (testing "coerce, filter and alter using xform"
      (let [out-plain (m/find-all db-spec coll :xform (comp (map coerce-my-record!)
                                                            (filter (comp seq :set))
                                                            (map #(update % :map assoc :k :v))))]
        (is (= [{:set #{"a" "b" "c"}
                 :vec [1 2 3]
                 :map {:a :x
                       :b :y
                       :k :v}}]
               (mapv #(dissoc % :_id) out-plain)))))))
