(ns murmeli.core-test
  (:require [clojure.spec.test.alpha :as stest]
            [clojure.test :as test :refer [deftest is testing]]
            [matcher-combinators.matchers :as matchers]
            [matcher-combinators.test]
            [murmeli.core :as m]
            [murmeli.impl.convert :as mc]
            [murmeli.operators :refer [$addFields $eq $exists $group $gt $jsonSchema $lt $match $project $push $set
                                       $sum]]
            [murmeli.specs]
            [murmeli.test.utils :as test-utils]
            [murmeli.validators.schema :as vs]
            [schema.coerce :as sc]
            [schema.core :as s :refer [defschema]])
  (:import [clojure.lang IReduceInit Ratio]
           [com.mongodb MongoCommandException]
           [org.bson BsonBinarySubType BsonReader BsonWriter]
           [org.bson.codecs Codec DecoderContext EncoderContext]
           [org.bson.types Binary ObjectId]))

(set! *warn-on-reflection* true)

(test/use-fixtures :once test-utils/container-cleanup-fixture)

(stest/instrument)

(defn- reducible?
  [r]
  (instance? IReduceInit r))

(defn get-coll
  "Return a random unique collection name for a test"
  []
  (keyword (str "coll-" (gensym))))

(deftest id-test
  (is (not (m/id? nil)))
  (is (not (m/id? "")))
  (is (m/id? "667c471cea82561061cb1a96"))
  (is (m/id? (m/create-id)))
  (is (m/object-id? (m/create-object-id))))

(deftest connect-test
  (test-utils/with-matrix
    (let [original-conn (test-utils/get-conn)]
      (is (m/connected? original-conn))
      ;; Create a new parallel connection
      (let [new-db-spec (test-utils/db-spec (force test-utils/*container*))]
        (is (not (m/connected? new-db-spec)) "Initially not connected")
        (let [connected (m/connect-client! new-db-spec)]
          (is (m/connected? connected))
          (m/disconnect! connected)))
      (is (m/connected? original-conn)))))

(deftest db-test
  (test-utils/with-matrix
    (let [conn (test-utils/get-conn)]
      (is (reducible? (m/list-db-names-reducible conn)))
      (testing "list database names"
        (let [names (m/list-db-names conn)]
          (is (every? keyword? names)))
        (let [names (m/list-db-names conn :keywords? false)]
          (is (every? string? names))))
      (testing "list databases"
        (is (reducible? (m/list-dbs-reducible conn)))
        (doseq [{:keys [name sizeOnDisk empty]} (m/list-dbs conn)]
          (is (string? name))
          (is (int? sizeOnDisk))
          (is (not empty)))
        (let [data (->> (m/list-dbs-reducible conn {:comment "reading dbs"
                                                    :query   {:name {$eq :admin}}})
                        (eduction (map #(update % :name keyword)))
                        (into []))]
          (is (match? [{:name       :admin
                        :sizeOnDisk int?
                        :empty      false}]
                      data)))))))

(deftest with-db-test
  (test-utils/with-matrix
    (let [conn     (test-utils/get-conn)
          with-foo (m/with-db conn "foo")]
      (is (not= conn with-foo))
      (is (= with-foo (m/with-db with-foo "foo"))))))

(def ratio-codec
  "A `Codec` for `Ratio` which encodes values as doubles."
  (reify Codec
    (getEncoderClass [_this] Ratio)
    (^void encode [_this ^BsonWriter writer ratio ^EncoderContext _ctx]
     (.writeDouble writer (double ratio)))
    (decode [_this ^BsonReader _reader ^DecoderContext _ctx]
      (throw (ex-info "not implemented" {})))))

(deftest with-registry-test
  (test-utils/with-matrix
    (let [conn              (test-utils/get-conn)
          coll              (get-coll)
          original-registry (m/get-registry conn)]
      (is original-registry)
      (is (= conn (m/with-registry conn original-registry)))
      (testing "user supplied codec"
        (let [new-registry  (mc/join-registries
                              original-registry
                              (mc/codecs->registry
                                ratio-codec))
              conn          (m/with-registry conn new-registry)
              {:keys [_id]} (m/insert-one! conn coll {:foo (/ 1 2)})]
          (is (= {:_id _id
                  :foo 0.5}
                 (m/find-by-id conn coll _id))))))))

(deftest drop-db-test
  (test-utils/with-matrix
    (let [conn         (test-utils/get-conn)
          get-db-names (fn [conn]
                         (->> (m/list-dbs conn)
                              (map :name)
                              (into #{})))
          db-name  (str (gensym "new-db-"))]
      (testing "initial state"
        (is (not (contains? (get-db-names conn) db-name))))
      (testing "dropping an unknown db does nothing"
        (is (nil? (m/drop-db! conn (str (random-uuid))))))
      (testing "create db with coll and drop it"
        (let [conn (m/with-db conn db-name)
              coll (get-coll)]
          (is (not (contains? (get-db-names conn) db-name))
              "Retrieving a database does not create it")
          (m/create-collection! conn coll)
          (is (contains? (get-db-names conn) db-name))
          (is (nil? (m/drop-db! conn db-name)))
          (is (not (contains? (get-db-names conn) db-name))))))))

(deftest list-collection-names-test
  (test-utils/with-matrix
    (let [conn   (test-utils/get-conn)
          coll-1 (get-coll)
          coll-2 (get-coll)]
      (is (reducible? (m/list-collection-names-reducible conn)))
      (testing "basic stuff"
        (is (= #{} (m/list-collection-names conn)))
        (m/create-collection! conn coll-1)
        (is (= #{coll-1} (m/list-collection-names conn)))
        (m/create-collection! conn coll-2)
        (is (= #{coll-1 coll-2} (m/list-collection-names conn)))
        (m/drop-collection! conn coll-1)
        (is (= #{coll-2} (m/list-collection-names conn)))
        (m/drop-collection! conn coll-2)
        (is (= #{} (m/list-collection-names conn))))
      (testing "extra options"
        (m/create-collection! conn coll-1)
        (m/create-collection! conn coll-2)
        (testing "batch size"
          (is (= #{coll-1 coll-2} (m/list-collection-names conn :batch-size 1)))
          (is (thrown-with-msg? MongoCommandException
                                #"'batchSize' value must be >= 0"
                                (m/list-collection-names conn :batch-size -1))))
        (testing "max-time-ms"
          (is (= #{coll-1 coll-2} (m/list-collection-names conn :max-time-ms 100))))
        (testing "keywords?"
          (is (= #{(name coll-1) (name coll-2)} (m/list-collection-names conn :keywords? false))))))))

(deftest list-collections-test
  (test-utils/with-matrix
    (let [conn   (test-utils/get-conn)
          coll-1 (get-coll)]
      (is (reducible? (m/list-collections-reducible conn)))
      (testing "basic stuff"
        (is (empty? (m/list-collections conn)))
        (m/create-collection! conn coll-1)
        (is (match? [{:name    (name coll-1)
                      :type    "collection"
                      :options {}
                      :info    {:readOnly false
                                :uuid     (fn [^Binary obj]
                                            (and (instance? Binary obj)
                                                 (= (.getValue BsonBinarySubType/UUID_STANDARD)
                                                    (.getType obj))))}
                      :idIndex {:v    2
                                :key  {:_id 1}
                                :name "_id_"}}]
                    (m/list-collections conn)))))))

(deftest simple-insert-test
  (test-utils/with-matrix
    (testing "inserting document"
      (let [conn (test-utils/get-conn)
            coll (get-coll)
            id   (:_id (m/insert-one! conn coll {:foo 123}))]
        ;; By default the generated ID will be a proper ObjectId
        (is (m/object-id? id))
        (is (= 1 (m/count-collection conn coll)))
        (is (= {:_id id
                :foo 123}
               (m/find-one conn coll :query {:_id id})
               (m/find-by-id conn coll id)
               (m/find-by-id conn coll (ObjectId. (str id)))))
        (let [id-2 (:_id (m/insert-one! conn coll {:foo 2
                                                   :_id (m/create-id)}))]
          (is (m/id? id-2)))

        (testing "insert with escape characters"
          (testing "string values can contain NULLs"
            (let [id  (:_id (m/insert-one! conn coll {"bar" "foo \0 asd"}))
                  res (:bar (m/find-by-id conn coll id))]
              (is (= res "foo \0 asd"))))
          (testing "and we can choose to sanitize them"
            (let [id  (:_id (m/insert-one! conn coll {"bar" "foo \0 asd"} {:sanitize-strings? true}))
                  res (:bar (m/find-by-id conn coll id))]
              (is (= res "foo  asd"))))
          (testing "... but not field names as they are stored as C strings"
            (is (thrown-with-msg?
                  RuntimeException
                  #"BSON cstring 'bar .{1}' is not valid because it contains a null character at index 4"
                  (m/insert-one! conn coll {"bar \0" "foo"})))))))))

(deftest simple-insert-many-test
  (test-utils/with-matrix
    (testing "inserting documents"
      (let [conn                     (test-utils/get-conn)
            coll                     (get-coll)
            [id-1 id-2 id-3 :as ids] (:_ids (m/insert-many! conn coll [{:foo 1}
                                                                       {:foo 2}
                                                                       {:foo 3}]))]
        (is (every? m/object-id? ids))
        (is (= 3 (count ids)))
        (is (= 3 (m/count-collection conn coll)))
        (is (= [{:_id id-1 :foo 1}
                {:_id id-2 :foo 2}
                {:_id id-3 :foo 3}] (m/find-all conn coll)))
        (is (= {:_id id-1 :foo 1} (m/find-one conn coll :query {:_id id-1})))
        (is (= {:_id id-2 :foo 2} (m/find-one conn coll :query {:_id id-2})))
        (is (= {:_id id-3 :foo 3} (m/find-one conn coll :query {:_id id-3})))))))

(deftest count-with-query-test
  (test-utils/with-matrix
    (testing "count with query"
      (let [conn (test-utils/get-conn)
            coll (get-coll)]
        (is (m/object-id? (:_id (m/insert-one! conn coll {:foo 123}))))
        (is (= 1 (m/count-collection conn coll)))
        (is (= 0 (m/count-collection conn coll :query {:foo {$lt 100}})))
        (is (= 1 (m/count-collection conn coll :query {:foo {$lt 200}})))
        (is (= 1 (m/estimated-count-collection conn coll)))))))

(deftest transaction-test
  (test-utils/with-matrix
    (testing "exception in transaction"
      (let [coll (get-coll)
            conn (test-utils/get-conn)]
        (is (zero? (m/count-collection conn coll)))
        (try
          (m/with-session [conn (m/with-client-session-options conn)]
            (m/insert-one! conn coll {:foo 123})
            (throw (ex-info "foo" {})))
          (catch Exception e
            (is (= "foo" (.getMessage e)))))
        (is (zero? (m/count-collection conn coll)))

        (testing "happy path"
          (m/with-session [conn (m/with-client-session-options conn)]
            (m/insert-one! conn coll {:foo 123})
            (is (= 1 (m/count-collection conn coll))))
          (is (= 1 (m/count-collection conn coll))))

        (testing "nested transactions"
          (m/with-session [conn (m/with-client-session-options conn)]
            (m/with-session [conn conn]
              (m/insert-one! conn coll {:foo 123})
              (is (= 2 (m/count-collection conn coll))))
            (is (= 2 (m/count-collection conn coll)))
            (m/insert-one! conn coll {:foo 123}))
          (is (= 3 (m/count-collection conn coll))))))))

(deftest update-one-test
  (test-utils/with-matrix
    (let [coll   (get-coll)
          conn   (test-utils/get-conn)
          [id-1] (:_ids (m/insert-many! conn coll [{:foo 1}]))]
      (testing "update single document"
        (is (= {:modified 1
                :matched  1}
               (m/update-one! conn coll {:_id id-1} {$set {:foo 10}}))))
      (testing "same update again -> no changes"
        (is (= {:modified 0
                :matched  1}
               (m/update-one! conn coll {:_id id-1} {$set {:foo 10}}))))
      (testing "no match"
        (is (= {:modified 0
                :matched  0}
               (m/update-one! conn coll {:quuz "kukka"} {$set {:foo 10}}))))
      (testing "upsert"
        (is (match? {:modified 0
                     :matched  0
                     :_id      m/object-id?}
                    (m/update-one! conn coll {:quuz "kukka"} {$set {:foo 10}} {:upsert? true})))))))

(deftest update-many-test
  (test-utils/with-matrix
    (let [coll   (get-coll)
          conn   (test-utils/get-conn)
          [id-1] (:_ids (m/insert-many! conn coll [{:foo 1}
                                                   {:foo 2}
                                                   {:foo 3}]))]
      (testing "update single document"
        (is (= {:modified 1
                :matched  1}
               (m/update-many! conn coll {:_id id-1} {$set {:foo 10}}))))
      (testing "same update again -> no changes"
        (is (= {:modified 0
                :matched  1}
               (m/update-many! conn coll {:_id id-1} {$set {:foo 10}}))))
      (testing "no match"
        (is (= {:modified 0
                :matched  0}
               (m/update-many! conn coll {:quuz "kukka"} {$set {:foo 10}}))))
      (testing "update multiple documents"
        (is (= {:modified 2 ; id-1 already has :foo as 10
                :matched  3}
               (m/update-many! conn coll {:foo {$exists true}} {$set {:foo 10}}))))
      (testing "update multiple fields"
        (is (= {:modified 3
                :matched  3}
               (m/update-many! conn coll {:foo {$exists true}} {$set {:bar  1
                                                                      :quuz 1}}))))
      (testing "upsert"
        (is (match? {:modified 0
                     :matched  0
                     :_id      m/object-id?}
                    (m/update-many! conn coll {:quuz "kukka"} {$set {:foo 10}} {:upsert? true})))))))

(deftest find-test
  (test-utils/with-matrix
    (let [coll   (get-coll)
          conn   (test-utils/get-conn)
          id     (:_id (m/insert-one! conn coll {:foo 123}))
          id-2   (:_id (m/insert-one! conn coll {:bar "quuz"}))
          id-3   (:_id (m/insert-one! conn coll {:foo 200
                                                 :bar "aaaa"}))
          item-1 {:_id id
                  :foo 123}
          item-2 {:_id id-2
                  :bar "quuz"}
          item-3 {:_id id-3
                  :foo 200
                  :bar "aaaa"}]
      (is (m/object-id? id))
      (is (m/object-id? id-2))
      (is (m/object-id? id-3))
      (testing "find all"
        (let [results (m/find-all conn coll)]
          (is (= [item-1 item-2 item-3]
                 results)))
        (testing "projection"
          (testing "projection as a map"
            (let [results (m/find-all conn coll :projection {:_id 1})]
              (is (= [{:_id id} {:_id id-2} {:_id id-3}]
                     results)))
            (let [results (m/find-all conn coll :projection {:foo 1})]
              (is (= [{:_id id :foo 123} {:_id id-2} {:_id id-3 :foo 200}]
                     results))))
          (testing "projection as a vec"
            (let [results (m/find-all conn coll :projection [:foo :bar :baz])]
              (is (= [{:_id id :foo 123}
                      {:_id id-2 :bar "quuz"}
                      {:_id id-3 :foo 200 :bar "aaaa"}]
                     results)))))
        (testing "sorting"
          (let [results (m/find-all conn coll :sort [[:foo 1]])]
            (is (= [item-2 item-1 item-3]
                   results)))
          (let [results (m/find-all conn coll :sort [[:foo -1]])]
            (is (= [item-3 item-1 item-2]
                   results)))))
      (testing "find all by query"
        (is (empty? (m/find-all conn coll :query {:foo {$gt 1000}})))
        (let [results (m/find-all conn coll :query {:foo {$gt 5
                                                          $lt 150}})]
          (is (= [item-1]
                 results))))
      (testing "find one by query"
        (is (nil? (m/find-one conn coll :query {:foo {$gt 1000}})))
        (let [results (m/find-one conn coll :query {:foo {$gt 5
                                                          $lt 150}})]
          (is (= item-1 results)))
        (testing "find-one throws on multiple hits"
          (is (thrown-with-msg? RuntimeException
                                #"find-one found multiple results"
                                (m/find-one conn coll :query {:_id {$exists 1}}))))))))

(deftest str-coll-test
  (test-utils/with-matrix
    (let [coll   (get-coll)
          conn   (test-utils/get-conn)
          id     (:_id (m/insert-one! conn coll {"foo" 123}))
          id-2   (:_id (m/insert-one! conn coll {"bar" "quuz"}))
          id-3   (:_id (m/insert-one! conn coll {"foo" 200
                                                 "bar" "aaaa"}))
          item-1 {"_id" id
                  "foo" 123}
          item-2 {"_id" id-2
                  "bar" "quuz"}
          item-3 {"_id" id-3
                  "foo" 200
                  "bar" "aaaa"}]
      (testing "find all"
        (let [results (m/find-all conn coll :keywords? false)]
          (is (= [item-1 item-2 item-3]
                 results)))
        (testing "projection"
          (let [results (m/find-all conn coll :projection {"_id" 1} :keywords? false)]
            (is (= [{"_id" id} {"_id" id-2} {"_id" id-3}]
                   results)))
          (let [results (m/find-all conn coll :projection {"foo" 1} :keywords? false)]
            (is (= [{"_id" id "foo" 123} {"_id" id-2} {"_id" id-3 "foo" 200}]
                   results))))
        (testing "sorting"
          (let [results (m/find-all conn coll :sort [["foo" 1]] :keywords? false)]
            (is (= [item-2 item-1 item-3]
                   results)))
          (let [results (m/find-all conn coll :sort [["foo" -1]] :keywords? false)]
            (is (= [item-3 item-1 item-2]
                   results)))))
      (testing "find all by query"
        (is (empty? (m/find-all conn coll :query {"foo" {$gt 1000}} :keywords? false)))
        (let [results (m/find-all conn coll :query {"foo" {$gt 5
                                                           $lt 150}} :keywords? false)]
          (is (= [item-1]
                 results))))
      (testing "find one by query"
        (is (nil? (m/find-one conn coll :query {:foo {$gt 1000}} :keywords? false)))
        (let [results (m/find-one conn coll :query {:foo {$gt 5
                                                          $lt 150}} :keywords? false)]
          (is (= item-1 results)))
        (testing "find-one throws on multiple hits"
          (is (thrown-with-msg? RuntimeException
                                #"find-one found multiple results"
                                (m/find-one conn coll :query {"_id" {$exists 1}} :keywords? false))))))))

(deftest index-test
  (test-utils/with-matrix
    (let [coll     (get-coll)
          conn     (test-utils/get-conn)
          id-index {:name "_id_"
                    :key  {:_id 1}
                    :v    2}]
      (m/insert-one! conn coll {:foo 123})
      (testing "initially no indexes"
        (is (= [id-index]
               (m/list-indexes conn coll))))
      (testing "creating indexes"
        (m/create-index! conn coll
                         {:foo 1}
                         {:background? true
                          :index-name  "my-index"
                          :sparse?     true
                          :unique?     true})
        (m/create-index! conn coll
                         {:foo.bar 1}
                         {:index-name "my-index-2"})
        (m/create-index! conn coll
                         {:foo.quuz 1
                          :foo.quiz 1}
                         {:index-name "my-index-3"})
        (testing "create a text index"
          (m/create-index! conn coll
                           {:foo.text-1 "text"
                            :foo.text-2 "text"}
                           {:index-name        "my-index-4"
                            :default-language  "fi"
                            :language-override :my-lang
                            :weights           {:foo.text-1 10
                                                :foo.text-2 5}}))
        (testing "index with a PFE"
          (m/create-index! conn coll
                           {:foo 1}
                           {:index-name                "my-index-5"
                            :partial-filter-expression {:bar {$gt "7"}}}))
        (testing "index with collation options"
          (m/create-index! conn coll
                           {:foo.bar 1}
                           {:index-name        "my-index-6"
                            :collation-options {:alternate         :shifted
                                                :backwards?        false
                                                :case-first        :upper
                                                :case-sensitive?   true
                                                :locale            "fi"
                                                :max-variable      :space
                                                :normalize?        true
                                                :numeric-ordering? true
                                                :strength          :primary}}))
        (is (= [id-index
                {:background true
                 :key        {:foo 1}
                 :name       "my-index"
                 :sparse     true
                 :unique     true
                 :v          2}
                {:key  {:foo.bar 1}
                 :name "my-index-2"
                 :v    2}
                {:key  {:foo.quuz 1
                        :foo.quiz 1}
                 :name "my-index-3"
                 :v    2}
                {:default_language  "fi"
                 :key               {:_fts  "text"
                                     :_ftsx 1}
                 :language_override "my-lang"
                 :name              "my-index-4"
                 :textIndexVersion  3
                 :v                 2
                 :weights           {:foo.text-1 10
                                     :foo.text-2 5}}
                {:key                     {:foo 1}
                 :name                    "my-index-5"
                 :partialFilterExpression {:bar {:$gt "7"}}
                 :v                       2}
                {:collation {:alternate       "shifted"
                             :backwards       false
                             :caseFirst       "upper"
                             :caseLevel       true
                             :locale          "fi"
                             :maxVariable     "space"
                             :normalization   true
                             :numericOrdering true
                             :strength        1
                             :version         "57.1"}
                 :key       {:foo.bar 1}
                 :name      "my-index-6"
                 :v         2}]
               (m/list-indexes conn coll))))
      (testing "removing index"
        (testing "removing with wrong name fails"
          (let [^MongoCommandException e (is (thrown? MongoCommandException
                                                      (m/drop-index-by-name! conn coll "missing")))]
            (is (= 27 (.getErrorCode e)))))
        (testing "removing with correct name works"
          (m/drop-index-by-name! conn coll "my-index")
          (m/drop-index-by-name! conn coll "my-index-4")
          (m/drop-index-by-name! conn coll "my-index-5")
          (m/drop-index-by-name! conn coll "my-index-6")
          (is (= [id-index
                  {:name "my-index-2"
                   :key  {:foo.bar 1}
                   :v    2}
                  {:name "my-index-3"
                   :key  {:foo.quuz 1
                          :foo.quiz 1}
                   :v    2}]
                 (m/list-indexes conn coll))))
        (testing "removing with keys works"
          (m/drop-index! conn coll {:foo.bar 1})
          (is (= [id-index
                  {:name "my-index-3"
                   :key  {:foo.quuz 1
                          :foo.quiz 1}
                   :v    2}]
                 (m/list-indexes conn coll))))
        (testing "removing all indexes works"
          (m/drop-all-indexes! conn coll)
          (is (= [id-index]
                 (m/list-indexes conn coll))))))))

(deftest schema-test
  (test-utils/with-matrix
    (let [coll (get-coll)
          conn (test-utils/get-conn)
          id-0 (:_id (m/insert-one! conn coll {}))
          id-1 (:_id (m/insert-one! conn coll {:foo "just foo"}))
          id-2 (:_id (m/insert-one! conn coll {:foo "foo and a timestamp"
                                               :bar #inst "2024-06-01"}))
          id-3 (:_id (m/insert-one! conn coll {:bar #inst "2024-06-02"}))]

      (testing "one required field"
        (let [schema (vs/schema->json-schema
                       {:foo s/Str})]
          (is (= {:_id id-1
                  :foo "just foo"}
                 (m/find-one conn coll :query {$jsonSchema schema})))))

      (testing "two required fields"
        (let [schema (vs/schema->json-schema
                       {:foo s/Str
                        :bar s/Inst})]
          (is (= {:_id id-2
                  :foo "foo and a timestamp"
                  :bar #inst "2024-06-01"}
                 (m/find-one conn coll :query {$jsonSchema schema})))))

      (testing "one optional, one required"
        (let [schema (vs/schema->json-schema
                       {(s/optional-key :foo) s/Str
                        :bar                  s/Inst})]
          (is (= [{:_id id-2
                   :foo "foo and a timestamp"
                   :bar #inst "2024-06-01"}
                  {:_id id-3
                   :bar #inst "2024-06-02"}]
                 (m/find-all conn coll :query {$jsonSchema schema})))))

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
                 (m/find-all conn coll :query {$jsonSchema schema}))))))))

(defschema MyRecord
  {(s/optional-key :_id) s/Str
   :set                  #{s/Str}
   :vec                  [s/Int]
   :map                  {s/Keyword s/Keyword}})

(def valid-my-record! (s/validator MyRecord))
(def coerce-my-record! (sc/coercer! MyRecord sc/json-coercion-matcher))

(deftest external-xform-test
  (test-utils/with-matrix
    (let [coll  (get-coll)
          conn  (test-utils/get-conn)
          input [{:_id (m/create-id)
                  :set #{}
                  :vec []
                  :map {}}
                 {:_id (m/create-id)
                  :set #{"a" "b" "c"}
                  :vec [1 2 3]
                  :map {:a :x
                        :b :y}}]]
      (is (nil? (run! valid-my-record! input)))
      (is (= 2 (count (:_ids (m/insert-many! conn coll input)))))
      (testing "read out as-is"
        (let [out-plain (m/find-all conn coll)]
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
      (is (reducible? (m/find-reducible conn coll)))
      (testing "coerce using xform"
        (let [out-plain (into []
                              (map coerce-my-record!)
                              (m/find-reducible conn coll))]
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
      (testing "add processing step with eduction"
        (let [out-plain (reduce conj
                                []
                                (->> (m/find-reducible conn coll)
                                     (eduction (map (fn [item] (assoc item :foo true))))))]
          (is (match? [{:_id m/id?
                        :set []
                        :vec []
                        :map {}
                        :foo true}
                       {:_id m/id?
                        :set ["a" "b" "c"]
                        :vec [1 2 3]
                        :map {:a "x"
                              :b "y"}
                        :foo true}]
                      out-plain))))
      (testing "coerce, filter and alter using xform"
        (let [out-plain (into []
                              (comp (map coerce-my-record!)
                                    (filter (comp seq :set))
                                    (map #(update % :map assoc :k :v)))
                              (m/find-reducible conn coll))]
          (is (= [{:set #{"a" "b" "c"}
                   :vec [1 2 3]
                   :map {:a :x
                         :b :y
                         :k :v}}]
                 (mapv #(dissoc % :_id) out-plain)))))
      (testing "example of run! with side-effect"
        (run! (fn [item]
                (println item))
              (m/find-reducible conn coll))))))

(deftest find-one-and-delete-test
  (test-utils/with-matrix
    (let [coll (get-coll)
          conn (test-utils/get-conn)]
      (is (nil? (m/find-one-and-delete! conn coll {:foo 1})))
      (m/insert-many! conn coll [{:foo 1 :data "bar"}
                                 {:foo 2 :data "quuz"}])
      (is (= 2 (m/count-collection conn coll)))
      (is (match? {:_id m/object-id? :foo 1 :data "bar"}
                  (m/find-one-and-delete! conn coll {:foo 1})))
      (is (= 1 (m/count-collection conn coll)))
      (is (match? {:_id m/object-id? :foo 2 :data "quuz"}
                  (m/find-one-and-delete! conn coll {:foo 2})))
      (is (zero? (m/count-collection conn coll))))))

(deftest find-one-and-replace-test
  (test-utils/with-matrix
    (let [coll (get-coll)
          conn (test-utils/get-conn)]
      (is (nil? (m/find-one-and-replace! conn coll {:foo 1} {:foo 4})))
      (m/insert-many! conn coll [{:foo 1 :data "bar"}
                                 {:foo 2 :data "quuz"}])
      (is (= 2 (m/count-collection conn coll)))

      (testing "replace document and return original"
        (is (match? {:_id m/object-id? :foo 1 :data "bar"}
                    (m/find-one-and-replace! conn coll {:foo 1} {:foo 4 :data "bar 2"} :return :before)))
        (is (match? {:_id m/object-id? :foo 4 :data "bar 2"}
                    (m/find-one conn coll :query {:foo 4})))
        (is (= 2 (m/count-collection conn coll))))

      (testing "replace document and return new version"
        (is (match? {:_id m/object-id? :foo 2 :data "quuz 2"}
                    (m/find-one-and-replace! conn coll {:foo 2} {:foo 2 :data "quuz 2"} :return :after)))
        (is (match? {:_id m/object-id? :foo 2 :data "quuz 2"}
                    (m/find-one conn coll :query {:foo 2})))
        (is (= 2 (m/count-collection conn coll)))))))

(deftest find-one-and-update-test
  (test-utils/with-matrix
    (let [coll (get-coll)
          conn (test-utils/get-conn)]
      (is (nil? (m/find-one-and-update! conn coll {:foo 1} {$set {:bar 2}})))
      (m/insert-many! conn coll [{:foo 1 :data "bar"}
                                 {:foo 2 :data "quuz"}])
      (is (= 2 (m/count-collection conn coll)))

      (testing "update document and return original"
        (is (match? {:_id m/object-id? :foo 1 :data "bar"}
                    (m/find-one-and-update! conn coll {:foo 1} {$set {:bar 2}} :return :before)))
        (is (match? {:_id m/object-id? :foo 1 :data "bar" :bar 2}
                    (m/find-one conn coll :query {:bar 2})))
        (is (= 2 (m/count-collection conn coll))))

      (testing "update document and return new version"
        (is (match? {:_id m/object-id? :foo 2 :data "quuz" :quuz 4}
                    (m/find-one-and-update! conn coll {:foo 2} {$set {:quuz 4}} :return :after)))
        (is (match? {:_id m/object-id? :foo 2 :data "quuz" :quuz 4}
                    (m/find-one conn coll :query {:foo 2})))
        (is (= 2 (m/count-collection conn coll)))))))

(deftest find-distinct-test
  (test-utils/with-matrix
    (let [coll (get-coll)
          conn (test-utils/get-conn)]
      (m/insert-many! conn coll [{:foo 1 :data "bar" :bar {:key 1} :quuz "this"}
                                 {:foo 2 :data "quuz" :bar {:key 2} :quuz "this"}])
      (testing "find-distinct-reducible"
        (is (reducible? (m/find-distinct-reducible conn coll :foo)))
        (is (= #{1 2}
               (into #{} (m/find-distinct-reducible conn coll :foo))))
        (is (= #{1 2}
               (reduce conj #{} (m/find-distinct-reducible conn coll :foo))))
        (is (= #{"1" "2"}
               (reduce (fn [acc v]
                         (conj acc (str v)))
                       #{}
                       (m/find-distinct-reducible conn coll :foo))))
        (is (= #{"1" "2"}
               (transduce
                 (map str)
                 conj
                 #{}
                 (m/find-distinct-reducible conn coll :foo)))))
      (testing "find-distict"
        (is (= #{} (m/find-distinct conn coll :i-dont-exist)))
        (is (= #{1 2} (m/find-distinct conn coll :foo)))
        (is (= #{"bar" "quuz"} (m/find-distinct conn coll :data)))
        (is (= #{{:key 1} {:key 2}} (m/find-distinct conn coll :bar)))
        (is (= #{"this"} (m/find-distinct conn coll :quuz)))))))

(deftest aggregate-test
  (test-utils/with-matrix
    (let [conn (test-utils/get-conn)
          usal {:country  "Spain"
                :city     "Salamanca"
                :name     "USAL"
                :location {:type        "Point"
                           :coordinates [-5.6722512,17 40.9607792]}
                :students [{ :year 2014 :number 24774 }
                           { :year 2015 :number 23166 }
                           { :year 2016 :number 21913 }
                           { :year 2017 :number 21715 }]}
          upsa {:country  "Spain"
                :city     "Salamanca"
                :name     "UPSA"
                :location {:type        "Point"
                           :coordinates [-5.6691191,17 40.9631732]}
                :students [{ :year 2014 :number 4788 }
                           { :year 2015 :number 4821 }
                           { :year 2016 :number 6550 }
                           { :year 2017 :number 6125 }]}]
      (testing "insert test data"
        (m/drop-collection! conn :universities)
        (m/drop-collection! conn :courses)
        (m/insert-many! conn :universities [usal upsa])
        (m/insert-many! conn
                        :courses
                        [{:university "USAL"
                          :name       "Computer Science"
                          :level      "Excellent"}
                         {:university "USAL"
                          :name       "Electronics"
                          :level      "Intermediate"}
                         {:university "USAL"
                          :name       "Communication"
                          :level      "Excellent"}]))
      (is (reducible? (m/aggregate-reducible! conn :universities [])))
      (testing "$match"
        (is (match? [(assoc usal :_id m/object-id?)
                     (assoc upsa :_id m/object-id?)]
                    (m/aggregate! conn :universities [{$match {:country "Spain"
                                                               :city    "Salamanca"}}]))))
      (testing "$project"
        (is (= [(dissoc usal :students :location)
                (dissoc upsa :students :location)]
               (m/aggregate! conn :universities [{$project {:_id     0
                                                            :country 1
                                                            :city    1
                                                            :name    1}}]))))
      (testing "$group"
        (is (match? (matchers/in-any-order [{:_id "UPSA" :totaldocs 1}
                                            {:_id "USAL" :totaldocs 1}])
                    (m/aggregate! conn :universities [{$group {:_id       "$name"
                                                               :totaldocs {$sum 1}}}])))))))

(defn- wait-for
  [check-fn timeout-ms]
  (let [deadline-ms (+ timeout-ms (System/currentTimeMillis))]
    (loop []
      (let [done?    (check-fn)
            timeout? (< deadline-ms (System/currentTimeMillis))]
        (if (or done? timeout?)
          done?
          (do
            (Thread/sleep 50)
            (recur)))))))

(deftest watch-test
  (test-utils/with-matrix
    (let [conn     (test-utils/get-conn)
          coll     (get-coll)
          *changes (atom [])
          fut      (future
                     (->> (m/watch-collection conn coll {:full-document               :update-lookup
                                                         :full-document-before-change :when-available})
                          (run! (fn [doc] (swap! *changes conj doc)))))]

      ;; We need to enable the pre- and post- images
      (m/create-collection! conn coll {:change-stream-options {:enabled? true}})

      (let [id (:_id (m/insert-one! conn coll {:foo "bar" :val 1 :arr [1 2]}))]
        (m/update-one! conn coll {:foo "bar"} {$set  {:val 42}
                                               $push {:arr 42}})

        (is (wait-for (fn [] (= 2 (count (deref *changes)))) 5000))

        (future-cancel fut)

        (let [changes (deref *changes)]
          (is (= 2 (count changes)))
          (is (match?
                {:database-name  string?
                 :document-key   {:_id id}
                 :full-document  {:_id id
                                  :foo "bar"
                                  :val 1
                                  :arr [1 2]}
                 :namespace      {:collection-name (name coll)
                                  :database-name   string?}
                 :operation-type :insert
                 :resume-token   {:_data string?}
                 :wall-time      inst?}
                (nth changes 0)))
          (is (match?
                {:database-name               string?
                 :document-key                {:_id id}
                 :full-document               {:_id id
                                               :foo "bar"
                                               :val 42
                                               :arr [1 2 42]}
                 :full-document-before-change {:_id id
                                               :foo "bar"
                                               :val 1
                                               :arr [1 2]}
                 :namespace                   {:collection-name (name coll)
                                               :database-name   string?}
                 :operation-type              :update
                 :resume-token                {:_data string?}
                 :update-description          {:updated-fields {:val   42
                                                                :arr.2 42}}
                 :wall-time                   inst?}
                (nth changes 1))))))))

(deftest watch-pipeline-test
  (test-utils/with-matrix
    (let [conn     (test-utils/get-conn)
          coll     (get-coll)
          *changes (atom [])
          fut      (future
                     (->> (m/watch-collection conn coll {:pipeline                    [{$match {:fullDocument.val 1}}
                                                                                       {$project {:fullDocument.val 0}}
                                                                                       {$addFields {:quuz 15}}]
                                                         :full-document               :update-lookup
                                                         :full-document-before-change :when-available})
                          (run! (fn [doc] (swap! *changes conj doc)))))]

      ;; We need to enable the pre- and post- images
      (m/create-collection! conn coll {:change-stream-options {:enabled? true}})

      (let [id (:_id (m/insert-one! conn coll {:foo "bar" :val 1 :arr [1 2]}))]
        ;; change stream ignores update as `val` is no longer 1
        (m/update-one! conn coll {:foo "bar"} {$set  {:val 42}
                                               $push {:arr 42}})

        (is (wait-for (fn [] (= 1 (count (deref *changes)))) 5000))

        (future-cancel fut)

        (let [changes (deref *changes)]
          (is (= 1 (count changes)))
          (is (match?
                {:database-name  string?
                 :document-key   {:_id id}
                 :extra-elements {:quuz 15} ; Added field
                 :full-document  {:_id id
                                  :foo "bar"
                                  ;; :val is excluded from the projection
                                  :arr [1 2]}
                 :namespace      {:collection-name (name coll)
                                  :database-name   string?}
                 :operation-type :insert
                 :resume-token   {:_data string?}
                 :wall-time      inst?}
                (nth changes 0))))))))
