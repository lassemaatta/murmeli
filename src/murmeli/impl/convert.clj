(ns murmeli.impl.convert
  "Convert Mongo BSON to/from Clojure data."
  (:import [clojure.lang APersistentMap
                         APersistentSet
                         APersistentVector
                         Keyword
                         PersistentArrayMap
                         PersistentHashMap
                         PersistentVector
                         Symbol]
           [com.mongodb MongoClientSettings]
           [java.util ArrayList]
           [java.util.regex Pattern]
           [org.bson BsonDocument
                     BsonDocumentWrapper
                     BsonObjectId
                     BsonReader
                     BsonType
                     BsonValue
                     BsonWriter
                     Document
                     UuidRepresentation]
           [org.bson.codecs BsonTypeClassMap
                            BsonValueCodecProvider
                            Codec
                            DecoderContext
                            EncoderContext
                            UuidCodecProvider]
           [org.bson.codecs.configuration CodecProvider CodecRegistries CodecRegistry]
           [org.bson.types ObjectId]))

;; https://github.com/mongodb/mongo-java-driver/tree/main/bson/src/main/org/bson/codecs

(set! *warn-on-reflection* true)

(defn id?
  [^String id]
  (boolean (and (string? id) (ObjectId/isValid id))))

(defn ->object-id
  "Coerce value to an `ObjectId`."
  ^ObjectId [value]
  (cond
    (instance? ObjectId value)     value
    (instance? BsonObjectId value) (.getValue ^BsonObjectId value)
    (string? value)                (ObjectId. ^String value)
    :else                          (throw (ex-info "Not an object-id" {:value value}))))

(defn object-id->bson
  ^BsonObjectId [^ObjectId id]
  (BsonObjectId. id))

(defn bson-value->document-id
  "The inserted ID is either a `BsonObjectId` or `BsonString`"
  [^BsonValue v]
  (let [t (.getBsonType v)]
    (cond
      (= t BsonType/STRING)    (-> v .asString .getValue)
      (= t BsonType/OBJECT_ID) (-> v .asObjectId .getValue))))

(defn map->bson
  "Convert a map to a `Bson`, which can produce a `BsonDocument`."
  ^BsonDocumentWrapper [m ^CodecRegistry registry]
  {:pre [(map? m) registry]}
  (BsonDocumentWrapper/asBsonDocument m registry))

(defn map->document
  "Convert a map to a `Document`."
  ^Document [m ^CodecRegistry registry]
  (let [bson   (map->bson m registry)
        codec  (.get registry Document)
        reader (.asBsonReader bson)
        ctx    (-> (DecoderContext/builder) .build)]
    (.decode codec reader ctx)))

(defn bson-document->map
  [^BsonDocument bson ^CodecRegistry registry]
  (let [codec  (.get registry PersistentHashMap)
        reader (.asBsonReader bson)
        ctx    (-> (DecoderContext/builder) .build)]
    (.decode codec reader ctx)))

(defn document->map
  "Convert a `Document` to a map."
  [^Document doc ^CodecRegistry registry]
  (-> doc
      (.toBsonDocument BsonDocument registry)
      (bson-document->map registry)))

;; TODO: pass this through options when building registry
(def overrides (BsonTypeClassMap. {BsonType/ARRAY              PersistentVector
                                   BsonType/DOCUMENT           PersistentHashMap
                                   BsonType/REGULAR_EXPRESSION Pattern}))

(defn bson-type->class
  [^BsonType bson-type ^BsonTypeClassMap overrides]
  (or (.get overrides bson-type)
      (BsonValueCodecProvider/getClassForBsonType bson-type)))

(defn- make-map-codec
  "Build `Codec` for `APersistentMap`."
  ^Codec [^CodecRegistry registry
          {:keys [keywords?
                  allow-qualified?]}
          {:keys [on-read-init
                  on-assoc
                  on-read-finished]}]
  (reify Codec
    (getEncoderClass [_this] APersistentMap)
    (^void encode [_this ^BsonWriter writer m ^EncoderContext ctx]
     (.writeStartDocument writer)
     (run! (fn [[k v]]
             (when (and (qualified-ident? k) (not allow-qualified?))
               (throw (ex-info "Cannot serialize qualified map keys" {:k k})))
             (.writeName writer (name k))
             (if (nil? v)
               (.writeNull writer)
               (let [clazz (class v)
                     c     (.get registry clazz)]
                 (.encodeWithChildContext ctx c writer v))))
           m)
     (.writeEndDocument writer))
    (decode [_this ^BsonReader reader ^DecoderContext ctx]
      (.readStartDocument reader)
      (let [m (loop [m* (on-read-init)]
                (cond
                  ;; Finished?
                  (= BsonType/END_OF_DOCUMENT (.readBsonType reader))
                  (on-read-finished m*)
                  ;; Read nil?
                  (= BsonType/NULL (.getCurrentBsonType reader))
                  (let [k (.readName reader)
                        k (if keywords? (keyword k) k)]
                    (.readNull reader)
                    (recur (on-assoc m* k nil)))
                  :else
                  (let [current (.getCurrentBsonType reader)
                        clazz   (bson-type->class current overrides)
                        codec   (.get registry clazz)
                        k       (.readName reader)
                        k       (if keywords? (keyword k) k)
                        v       (.decode codec reader ctx)]
                    (recur (on-assoc m* k v)))))]
        (.readEndDocument reader)
        m))))

(defn map-codec
  "Build `Codec` for `APersistentMap`."
  ^Codec [^CodecRegistry registry {:keys [retain-order?] :as opts}]
  (make-map-codec registry opts
                  (if retain-order?
                    {:on-read-init     (fn [] (ArrayList. 32))
                     :on-assoc         (fn [^ArrayList l k v]
                                         ;; Construct an array of [K1, V1, K2, V2, K3..]
                                         (.add l k)
                                         (.add l v)
                                         l)
                     :on-read-finished (fn [^ArrayList l]
                                         ;; `.toArray` allocates a new(!) array and
                                         ;; the persistent array map takes ownership of it.
                                         (PersistentArrayMap. (.toArray l)))}
                    ;; Decode into a transient map, key order not guaranteed
                    {:on-read-init     (fn [] (transient {}))
                     :on-assoc         (fn [m k v] (assoc! m k v))
                     :on-read-finished (fn [m] (persistent! m))})))

(defn- write-coll-children!
  [^BsonWriter writer ^CodecRegistry registry ^EncoderContext ctx xs]
  (run! (fn [x]
          (if (nil? x)
            (.writeNull writer)
            (let [clazz (class x)
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

(defn keyword-codec
  "A `Codec` for `Keyword`."
  [{:keys [allow-qualified?]}]
  (reify Codec
    (getEncoderClass [_this] Keyword)
    (^void encode [_this ^BsonWriter writer k ^EncoderContext _ctx]
     (when (and (qualified-keyword? k) (not allow-qualified?))
       (throw (ex-info "Cannot serialize qualified keywords" {:k k})))
     (.writeString writer (name k)))
    (decode [_this ^BsonReader reader ^DecoderContext _ctx]
      (keyword (.readString reader)))))

(defn symbol-codec
  "A `Codec` for `Symbol`."
  [{:keys [allow-qualified?]}]
  (reify Codec
    (getEncoderClass [_this] Symbol)
    (^void encode [_this ^BsonWriter writer sym ^EncoderContext _ctx]
     (when (and (qualified-symbol? sym) (not allow-qualified?))
       (throw (ex-info "Cannot serialize qualified symbols" {:sym sym})))
     (.writeString writer (name sym)))
    (decode [_this ^BsonReader reader ^DecoderContext _ctx]
      (symbol (.readString reader)))))

(def sanitizing-string-codec
  "A `Codec` for `String` which sanitizes NULLs.
  This isn't stricly necessary (anymore) due to https://github.com/mongodb/mongo-java-driver/pull/786."
  (reify Codec
    (getEncoderClass [_this] String)
    (^void encode [_this ^BsonWriter writer s ^EncoderContext _ctx]
     (.writeString writer (.replaceAll ^String s "\0" "")))
    (decode [_this ^BsonReader reader ^DecoderContext _ctx]
      (.readString reader))))

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
  [{:keys [sanitize-strings?] :as opts}]
  (reify CodecProvider
    (^Codec get [_this ^Class clazz ^CodecRegistry registry]
     (when clazz
       (cond
         (= Object clazz)                            (object-codec registry)
         (= Keyword clazz)                           (keyword-codec opts)
         (= Symbol clazz)                            (symbol-codec opts)
         (and sanitize-strings?
              (= String clazz))                      sanitizing-string-codec
         (.isAssignableFrom APersistentMap clazz)    (map-codec registry opts)
         (.isAssignableFrom APersistentSet clazz)    (set-codec registry)
         (.isAssignableFrom APersistentVector clazz) (vector-codec registry))))))

(def default-options {:allow-qualified?  false
                      :keywords?         true
                      :sanitize-strings? false
                      :retain-order?     false})

(def registry-options-keys (set (keys default-options)))

(defn join-registries
  "Combine multiple `CodecRegistry`s into a single `CodecRegistry`."
  ^CodecRegistry [& registries]
  (CodecRegistries/fromRegistries
    ^"[Lorg.bson.codecs.configuration.CodecRegistry;"
    (into-array CodecRegistry registries)))

(defn codecs->registry
  "Combine multiple `Codecs`s into a single `CodecRegistry`."
  ^CodecRegistry [& codecs]
  (CodecRegistries/fromCodecs
    ^"[Lorg.bson.codecs.Codec;"
    (into-array Codec codecs)))

(defn providers->registry
  "Combine multiple `Provider`s into a single `CodecRegistry`."
  ^CodecRegistry [& providers]
  (CodecRegistries/fromProviders
    ^"[Lorg.bson.codecs.configuration.CodecProvider;"
    (into-array CodecProvider providers)))

(defn registry
  "Construct a `CodecRegistry` for converting between Java classes and BSON
  Options:
  * `allow-qualified?`: Accept qualified idents (keywords or symbols), even though we discard the namespace
  * `keywords?`: Decode map keys as keywords instead of strings
  * `sanitize-strings?`: Remove NULL characters from strings
  * `retain-order?`: If true, always decodes documents into an array-map. Retains original key order,
                     but slower lookup (vs. hashmap). If false, decodes into array-map or hash-map."
  {:arglists '([{:keys [allow-qualified?
                        keywords?
                        retain-order?
                        sanitize-strings?]}])}
  ^CodecRegistry [opts]
  (join-registries
    (providers->registry
      (clojure-provider opts)
      (UuidCodecProvider. UuidRepresentation/STANDARD))
    (MongoClientSettings/getDefaultCodecRegistry)))

(def default-registry (registry default-options))
