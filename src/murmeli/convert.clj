(ns murmeli.convert
  (:import [clojure.lang APersistentMap
                         APersistentSet
                         APersistentVector
                         Keyword
                         PersistentHashMap
                         PersistentVector]
           [com.mongodb MongoClientSettings]
           [java.util Date]
           [java.util.regex Pattern]
           [org.bson BsonArray
                     BsonBinary
                     BsonBoolean
                     BsonDateTime
                     BsonDecimal128
                     BsonDocument
                     BsonDocumentWrapper
                     BsonDouble
                     BsonInt32
                     BsonInt64
                     BsonObjectId
                     BsonReader
                     BsonRegularExpression
                     BsonString
                     BsonType
                     BsonWriter
                     UuidRepresentation]
           [org.bson.codecs BsonValueCodecProvider
                            Codec
                            DecoderContext
                            EncoderContext
                            UuidCodecProvider]
           [org.bson.codecs.configuration CodecProvider CodecRegistries CodecRegistry]
           [org.bson.types Binary Decimal128 ObjectId]))

;; https://github.com/mongodb/mongo-java-driver/tree/main/bson/src/main/org/bson/codecs

(set! *warn-on-reflection* true)

(defn id?
  [^String id]
  (boolean (and (string? id) (ObjectId/isValid id))))

(defn map->bson
  "Convert a map to a `Bson`, which can produce a `BsonDocument`."
  ^BsonDocumentWrapper [m ^CodecRegistry registry]
  {:pre [(map? m) registry]}
  (BsonDocumentWrapper/asBsonDocument m registry))

(def overrides
  "Instead of decoding values as Bson* instances, use these overrides"
  {BsonArray             PersistentVector
   BsonBinary            Binary
   BsonBoolean           Boolean
   BsonDateTime          Date
   BsonDecimal128        Decimal128
   BsonDocument          PersistentHashMap
   BsonDouble            Double
   BsonInt32             Integer
   BsonInt64             Long
   BsonObjectId          ObjectId
   BsonRegularExpression Pattern
   BsonString            String})

(defn value->class
  [value overrides]
  (let [clazz (class value)]
    (get overrides clazz clazz)))

(defn bson-type->class
  [bson-type overrides]
  (let [clazz (BsonValueCodecProvider/getClassForBsonType bson-type)]
    (get overrides clazz clazz)))

(defn map-codec
  "Build `Codec` for `APersistentMap`."
  ^Codec [^CodecRegistry registry {:keys [keywords?]}]
  (reify Codec
    (getEncoderClass [_this] APersistentMap)
    (^void encode [_this ^BsonWriter writer m ^EncoderContext ctx]
     (.writeStartDocument writer)
     (run! (fn [[k v]]
             (.writeName writer (name k))
             (if (nil? v)
               (.writeNull writer)
               (let [clazz (value->class v overrides)
                     c     (.get registry clazz)]
                 (.encodeWithChildContext ctx c writer v))))
           m)
     (.writeEndDocument writer))
    (decode [_this ^BsonReader reader ^DecoderContext ctx]
      (.readStartDocument reader)
      (let [m (loop [m* (transient {})]
                (cond
                  ;; Finished?
                  (= BsonType/END_OF_DOCUMENT (.readBsonType reader))
                  (persistent! m*)
                  ;; Read nil?
                  (= BsonType/NULL (.getCurrentBsonType reader))
                  (let [k (.readName reader)
                        k (if keywords? (keyword k) k)]
                    (.readNull reader)
                    (recur (assoc! m* k nil)))
                  :else
                  (let [current (.getCurrentBsonType reader)
                        clazz   (bson-type->class current overrides)
                        codec   (.get registry clazz)
                        k       (.readName reader)
                        k       (if keywords? (keyword k) k)
                        v       (.decode codec reader ctx)]
                    (recur (assoc! m* k v)))))]
        (.readEndDocument reader)
        m))))

(defn- write-coll-children!
  [^BsonWriter writer ^CodecRegistry registry ^EncoderContext ctx xs]
  (run! (fn [x]
          (if (nil? x)
            (.writeNull writer)
            (let [clazz (value->class x overrides)
                  c     (.get registry clazz)]
              (.encodeWithChildContext ctx c writer x))))
        xs))

(defn vector-codec
  "Build `Codec` for `APersistentVector`."
  ^Codec [^CodecRegistry registry]
  (reify Codec
    (getEncoderClass [_this] APersistentVector)
    (^void encode [_this ^BsonWriter writer xs ^EncoderContext ctx]
     (.writeStartArray writer)
     (write-coll-children! writer registry ctx xs)
     (.writeEndArray writer))
    (decode [_this ^BsonReader reader ^DecoderContext ctx]
      (.readStartArray reader)
      (let [xs (loop [xs* (transient [])]
                 (cond
                   ;; Finished?
                   (= BsonType/END_OF_DOCUMENT (.readBsonType reader))
                   (persistent! xs*)
                   :else
                   (let [current (.getCurrentBsonType reader)]
                     (if (= BsonType/NULL current)
                       (do
                         (.readNull reader)
                         (recur (conj! xs* nil)))
                       (let [clazz (bson-type->class current overrides)
                             codec (.get registry clazz)
                             x     (.decode codec reader ctx)]
                         (recur (conj! xs* x)))))))]
        (.readEndArray reader)
        xs))))

(defn set-codec
  "Build `Codec` for `APersistentSet`."
  ^Codec [^CodecRegistry registry]
  (reify Codec
    (getEncoderClass [_this] APersistentSet)
    (^void encode [_this ^BsonWriter writer xs ^EncoderContext ctx]
     (.writeStartArray writer)
     (write-coll-children! writer registry ctx xs)
     (.writeEndArray writer))
    (decode [_this ^BsonReader reader ^DecoderContext ctx]
      (.readStartArray reader)
      (let [xs (loop [xs* (transient #{})]
                 (cond
                   ;; Finished?
                   (= BsonType/END_OF_DOCUMENT (.readBsonType reader))
                   (persistent! xs*)
                   :else
                   (let [current (.getCurrentBsonType reader)
                         clazz   (bson-type->class current overrides)
                         codec   (.get registry clazz)
                         x       (.decode codec reader ctx)]
                     (recur (conj! xs* x)))))]
        (.readEndArray reader)
        xs))))

(def keyword-codec
  "A `Codec` for `Keyword`."
  (reify Codec
    (getEncoderClass [_this] Keyword)
    (^void encode [_this ^BsonWriter writer k ^EncoderContext _ctx]
     (when (qualified-keyword? k)
       (throw (ex-info "Cannot serialize qualified keywords" {:k k})))
     (.writeString writer (name k)))
    (decode [_this ^BsonReader reader ^DecoderContext _ctx]
      (keyword (.readString reader)))))

(defn object-codec
  "A `Codec` for `Object`."
  [^CodecRegistry registry]
  (reify Codec
    (getEncoderClass [_this] Object)
    (^void encode [_this ^BsonWriter _writer v ^EncoderContext _ctx]
     (throw (ex-info "unsupported" {:v v})))
    (decode [_this ^BsonReader reader ^DecoderContext ctx]
      (let [current (.getCurrentBsonType reader)
            clazz   (bson-type->class current overrides)
            codec   (.get registry clazz)]
        (.decode codec reader ctx)))))

(defn- clojure-provider
  "Instantiate a `CodecProvider`, which can provide `Codec`s for various Clojure data structures"
  [opts]
  (reify CodecProvider
    (^Codec get [_this ^Class clazz ^CodecRegistry registry]
     (when clazz
       (cond
         (= Object clazz)                            (object-codec registry)
         (= Keyword clazz)                           keyword-codec
         (.isAssignableFrom APersistentMap clazz)    (map-codec registry opts)
         (.isAssignableFrom APersistentSet clazz)    (set-codec registry)
         (.isAssignableFrom APersistentVector clazz) (vector-codec registry))))))

(defn registry
  "Construct a `CodecRegistry` for converting between Java classes and BSON
  Options:
  - `keywords?`: Decode map keys as keywords instead of strings"
  {:arglists '([{:keys [keywords?]}])}
  [opts]
  (CodecRegistries/fromRegistries
    ^"[Lorg.bson.codecs.configuration.CodecRegistry;"
    (into-array CodecRegistry
                [(CodecRegistries/fromProviders
                   ^"[Lorg.bson.codecs.configuration.CodecProvider;"
                   (into-array CodecProvider
                               [(clojure-provider opts)
                                (UuidCodecProvider. UuidRepresentation/STANDARD)]))
                 (MongoClientSettings/getDefaultCodecRegistry)])))
