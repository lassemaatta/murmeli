(ns murmeli.gridfs-test
  (:require [clojure.java.io :as io]
            [clojure.spec.test.alpha :as stest]
            [clojure.test :as test :refer [deftest is testing]]
            [matcher-combinators.test]
            [murmeli.gridfs :as gfs]
            [murmeli.impl.gridfs :as gfs-impl]
            [murmeli.test.utils :as test-utils])
  (:import [com.mongodb.client.gridfs GridFSBucket]
           [java.io InputStream]
           [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

(stest/instrument)

(test/use-fixtures :once (test/join-fixtures
                           [test-utils/container-fixture
                            test-utils/db-fixture]))

(defn str->stream
  [^String s]
  (-> s .getBytes io/input-stream))

(deftest bucket-test
  (testing "default bucket"
    (let [{::gfs-impl/keys [^GridFSBucket bucket]} (-> (test-utils/get-conn)
                                                       (gfs/with-bucket))]
      (is (= "fs" (.getBucketName bucket)))))
  (testing "custom bucket"
    (let [{::gfs-impl/keys [^GridFSBucket bucket]} (-> (test-utils/get-conn)
                                                       (gfs/with-bucket "my-bucket"))]
      (is (= "my-bucket" (.getBucketName bucket))))))

(deftest upload-stream-test
  (let [conn (-> (test-utils/get-conn)
                 (gfs/with-bucket "my-bucket"))]
    (testing "file with no meta"
      (let [file-1   (str (random-uuid))
            source   (str->stream file-1)
            filename "plain-file.txt"
            id       (gfs/upload-stream! conn filename source)]
        (is (instance? ObjectId id))))
    (testing "file with meta"
      (let [file-2   (str (random-uuid))
            source   (str->stream file-2)
            filename "plain-file-with-meta.txt"
            doc      {:foo "some metadata"
                      :bar 42}
            id       (gfs/upload-stream! conn filename source {:doc doc})]
        (is (instance? ObjectId id))))
    (testing "find all"
      (let [results (gfs/find conn)]
        (is (match? [{:_id         #(instance? ObjectId %)
                      :chunk-size  261120
                      :filename    "plain-file.txt"
                      :length      36
                      :upload-date inst?}
                     {:_id         #(instance? ObjectId %)
                      :chunk-size  261120
                      :doc         {:foo "some metadata"
                                    :bar 42}
                      :filename    "plain-file-with-meta.txt"
                      :length      36
                      :upload-date inst?}]
                    (into [] results)))))
    (testing "rename"
      (let [[{:keys [_id]} :as results] (into [] (gfs/find conn {:query {:filename "plain-file.txt"}}))]
        (is (= 1 (count results)))
        (gfs/rename! conn _id "new-filename.txt")))
    (testing "find all"
      (let [results (gfs/find conn)]
        (is (match? [{:_id         #(instance? ObjectId %)
                      :chunk-size  261120
                      :filename    "new-filename.txt"
                      :length      36
                      :upload-date inst?}
                     {:_id         #(instance? ObjectId %)
                      :chunk-size  261120
                      :doc         {:foo "some metadata"
                                    :bar 42}
                      :filename    "plain-file-with-meta.txt"
                      :length      36
                      :upload-date inst?}]
                    (into [] results)))))
    (gfs/drop-bucket! conn)))

(deftest upload-download-test
  (let [conn (-> (test-utils/get-conn)
                 (gfs/with-bucket "my-bucket"))]
    (testing "upload file"
      (let [contents (str (random-uuid))
            source   (str->stream contents)
            filename "plain-file-with-meta.txt"
            doc      {:foo "some metadata"
                      :bar 42}
            id       (gfs/upload-stream! conn filename source {:doc doc})]
        (is (instance? ObjectId id))
        (let [expected-file {:_id         #(instance? ObjectId %)
                             :chunk-size  261120
                             :doc         {:foo "some metadata"
                                           :bar 42}
                             :filename    "plain-file-with-meta.txt"
                             :length      36
                             :upload-date inst?}]
          (testing "find file"
            (let [[file :as results] (into [] (gfs/find conn {:query {:_id id}}))]
              (is (= 1 (count results)))
              (is (match? expected-file file))))
          (testing "download file by id"
            (let [{:keys [input-stream file]} (gfs/download-stream conn {:id id})]
              (is (instance? InputStream input-stream))
              (is (= contents (slurp input-stream)))
              (is (match? expected-file file))))
          (testing "download file by filename"
            (let [{:keys [input-stream file]} (gfs/download-stream conn {:filename filename})]
              (is (instance? InputStream input-stream))
              (is (= contents (slurp input-stream)))
              (is (match? expected-file file)))))))
    (gfs/drop-bucket! conn)))

(deftest delete-test
  (let [conn (-> (test-utils/get-conn)
                 (gfs/with-bucket "my-bucket"))]
    (testing "upload file"
      (let [contents (str (random-uuid))
            source   (str->stream contents)
            filename "plain-file-with-meta.txt"
            doc      {:foo "some metadata"
                      :bar 42}
            id       (gfs/upload-stream! conn filename source {:doc doc})]
        (testing "find file"
          (let [results (into [] (gfs/find conn {:query {:_id id}}))]
            (is (= 1 (count results))))
          (let [results (into [] (gfs/find conn {:query {:filename filename}}))]
            (is (= 1 (count results)))))
        (gfs/delete! conn id)
        (testing "find file"
          (let [results (into [] (gfs/find conn {:query {:_id id}}))]
            (is (zero? (count results))))
          (let [results (into [] (gfs/find conn {:query {:filename filename}}))]
            (is (zero? (count results)))))))
    (gfs/drop-bucket! conn)))

(deftest drop-bucket-test
  (testing "custom bucket"
    (let [conn (-> (test-utils/get-conn)
                   (gfs/with-bucket "my-bucket"))]
      (gfs/drop-bucket! conn))))
