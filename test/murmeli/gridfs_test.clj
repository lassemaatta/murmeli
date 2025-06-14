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

(test/use-fixtures :once test-utils/container-cleanup-fixture)

(stest/instrument)

(defn str->stream
  [^String s]
  (-> s .getBytes io/input-stream))

(deftest bucket-test
  (test-utils/with-matrix
    (testing "default bucket"
      (let [{::gfs-impl/keys [^GridFSBucket bucket]} (-> (test-utils/get-conn)
                                                         (gfs/with-bucket))]
        (is (= "fs" (.getBucketName bucket)))))
    (testing "custom bucket"
      (let [{::gfs-impl/keys [^GridFSBucket bucket]} (-> (test-utils/get-conn)
                                                         (gfs/with-bucket "my-bucket"))]
        (is (= "my-bucket" (.getBucketName bucket)))))))

(deftest upload-stream-test
  (test-utils/with-matrix
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
                      (into [] results))))))))

(deftest upload-download-test
  (test-utils/with-matrix
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
                (is (match? expected-file file))))))))))

(deftest delete-test
  (test-utils/with-matrix
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
              (is (zero? (count results))))))))))

(deftest drop-bucket-test
  (test-utils/with-matrix
    (testing "custom bucket in conn"
      (let [conn (-> (test-utils/get-conn)
                     (gfs/with-bucket "my-bucket"))]

        (testing "upload file"
          (gfs/upload-stream! conn "dummy.txt" (str->stream "foo"))
          (is (= 1 (count (into [] (gfs/find conn))))
              "bucket contains a single file"))
        (gfs/drop-bucket! conn)
        (is (zero? (count (into [] (gfs/find conn))))
            "no files after dropping bucket")))

    (testing "delete other bucket"
      (let [conn (-> (test-utils/get-conn)
                     (gfs/with-bucket "my-bucket-2"))]

        (testing "upload file"
          (gfs/upload-stream! conn "dummy.txt" (str->stream "foo"))
          (is (= 1 (count (into [] (gfs/find conn))))
              "bucket contains a single file"))

        (testing "create and delete some other bucket by instance"
          (let [conn (gfs/with-bucket conn "other-bucket")]
            (testing "upload file"
              (gfs/upload-stream! conn "dummy-2.txt" (str->stream "foo"))
              (is (= 1 (count (into [] (gfs/find conn))))
                  "bucket contains a single file"))

            (let [bucket (gfs/create-bucket conn "other-bucket")]
              (gfs/drop-bucket! conn bucket)

              (is (zero? (count (into [] (gfs/find conn))))
                  "file in other-bucket removed"))))

        (testing "create and delete some other bucket by name"
          (let [bucket-name "yet-another-bucket"
                conn        (gfs/with-bucket conn bucket-name)]
            (testing "upload file"
              (gfs/upload-stream! conn "dummy-3.txt" (str->stream "foo"))
              (is (= 1 (count (into [] (gfs/find conn))))
                  "bucket contains a single file"))
            (gfs/drop-bucket! conn bucket-name)
            (is (zero? (count (into [] (gfs/find conn))))
                "file in yet-another-bucket removed")))

        (is (= 1 (count (into [] (gfs/find conn))))
            "original file in bucket")))))
