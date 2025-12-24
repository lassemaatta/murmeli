(ns murmeli.convert-test
  (:require [clojure.test :refer [are deftest is testing]]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.test.check.properties :as properties]
            [murmeli.impl.convert :as c]
            [murmeli.specs]
            [murmeli.test.generators :as mg])
  (:import [clojure.lang PersistentTreeMap]
           [java.nio ByteBuffer]
           [java.util.regex Pattern]
           [org.bson BsonBinaryReader BsonBinaryWriter ByteBufNIO]
           [org.bson.codecs Codec DecoderContext EncoderContext]
           [org.bson.codecs.configuration CodecRegistry]
           [org.bson.io BasicOutputBuffer ByteBufferBsonInput OutputBuffer]
           [org.bson.types ObjectId]))

(set! *warn-on-reflection* true)

(defn encode
  "Encode `value` using `registry` into an `OutputBuffer`"
  [^CodecRegistry registry value]
  (let [codec  (.get registry (class value))
        buffer (BasicOutputBuffer.)
        writer (BsonBinaryWriter. buffer)]
    (.encode codec writer value (-> (EncoderContext/builder) .build))
    {:codec codec :buffer buffer}))

(defn decode
  [{:keys [^OutputBuffer buffer ^Codec codec]}]
  (let [bb     (ByteBuffer/wrap (.toByteArray buffer))
        bbnio  (ByteBufNIO. bb)
        bbbi   (ByteBufferBsonInput. bbnio)
        reader (BsonBinaryReader. bbbi)]
    (.decode codec reader (-> (DecoderContext/builder) .build))))

(defn roundtrip
  ([value]
   (roundtrip (c/registry {:keywords? true}) value))
  ([registry value]
   (decode (encode registry value))))

(deftest roundtrip-test
  (let [oid (ObjectId.)]
    (are [value] (= value (roundtrip value))
      {:nil nil}
      {:bool true}
      {:int (int 1)}
      {:long (long 1)}
      {:double 1.2}
      {:object-id oid}
      {:string "foo"}
      {:inst #inst "2024-06-20"}
      {:list '(1 2 3)}
      {:map {:a 1
             :b "2"
             :c nil
             :d false}}
      {:hash-map (hash-map :foo 1)}
      {:array-map (array-map :bar 2)}
      {:tree-map (PersistentTreeMap/create {:quuz 3})}
      {:vector [1 "2" 3 nil false]})))

(deftest asymmetric-roundtrips
  (testing "pattern"
    (let [in           #"(?i)foobar\d+"
          ^Pattern out (:foo (roundtrip {:foo in}))]
      (is (instance? Pattern out))
      (is (= (.pattern in) (.pattern out)))
      (is (= (.flags in) (.flags out)))))
  (testing "keyword values"
    (let [in  {:foo [:bar]}
          out (:foo (roundtrip in))]
      (is (= ["bar"] out))))
  (testing "qualified keywords"
    (is (thrown-with-msg? RuntimeException
                          #"Cannot serialize qualified map keys"
                          (roundtrip {:foo/bar 1})))
    (is (thrown-with-msg? RuntimeException
                          #"Cannot serialize qualified keywords"
                          (roundtrip {:foo :bar/baz})))
    (is (= {:bar "nom"}
           (roundtrip (c/registry {:keywords? true :allow-qualified? true})
                      {:foo/bar :quuz/nom}))))
  (testing "qualified symbols"
    (is (thrown-with-msg? RuntimeException
                          #"Cannot serialize qualified map keys"
                          (roundtrip {'foo/bar 1})))
    (is (thrown-with-msg? RuntimeException
                          #"Cannot serialize qualified symbols"
                          (roundtrip {:foo 'bar/baz})))
    (is (= {:bar "nom"}
           (roundtrip (c/registry {:keywords? true :allow-qualified? true})
                      {'foo/bar 'quuz/nom}))))
  (testing "list -> JSON array -> vector"
    (let [in            {:l '(1 2 3)
                         :r (range 1 3)}
          {:keys [l r]} (roundtrip in)]
      (is (vector? l))
      (is (vector? r)))))

(deftest map-key-ordering-test
  (testing "short input"
    (let [ks    [:a :b :c :d :e :f]
          input (apply array-map (interleave (shuffle ks) (range)))]

      (testing "no ordering"
        (let [output (roundtrip input)]
          ;; BSON document -> transient -> array-map
          (is (= (keys input)
                 (keys output)))))
      (testing "with ordering"
        (let [output (roundtrip (c/registry {:keywords?     true
                                             :retain-order? true})
                                input)]
          ;; BSON document -> array-list -> array-map : key ordering is preserved
          (is (= (keys input)
                 (keys output)))))))
  (testing "long input"
    (let [ ks   [:a :b :c :d :e :f :g :h :i :j :k :l :m :n :o :p :q :r :s :t :u :v :x :y :z]
          input (apply array-map (interleave (shuffle ks) (range)))]
      (testing "no ordering"
        (let [output (roundtrip input)]
          ;; BSON document -> transient -> hash-map
          ;; technically the keys _might_ be in the correct order, just by chance
          (is (not= (keys input)
                    (keys output)))))
      (testing "with ordering"
        (let [output (roundtrip (c/registry {:keywords?     true
                                             :retain-order? true})
                                input)]
          ;; BSON document -> array-list -> array-map : key ordering is preserved
          (is (= (keys input)
                 (keys output))))))))

(defspec map->bson-test 50
  (properties/for-all [doc mg/doc-gen]
    (try
      ;; Use `.size` to force unwrapping the BSON which encodes it
      (.size (c/map->bson doc mg/registry))
      true
      (catch Exception _
        false))))
