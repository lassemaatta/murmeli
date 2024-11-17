(ns murmeli.test.generators
  (:require [clojure.spec.alpha :as s]
            [clojure.test.check.generators :as gen]
            [murmeli.convert :as mc]
            [murmeli.specs :as ms])
  (:import [org.bson.types ObjectId]))

(def uri-gen (gen/return "mongodb://localhost:27017"))

(def object-id-gen (gen/let [date gen/nat
                             counter gen/nat]
                     (ObjectId. (int date) (int counter))))

(def regex-gen (gen/return #"foo"))

(def registry (mc/registry {:keywords? true}))

(def doc-gen (binding [s/*recursion-limit* 1]
               (s/gen ::ms/document {::ms/object-id (constantly object-id-gen)
                                     ::ms/regex     (constantly regex-gen)})))

(def bson-gen (gen/let [doc doc-gen]
                (mc/map->bson doc registry)))
