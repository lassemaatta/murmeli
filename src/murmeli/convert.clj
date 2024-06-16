(ns murmeli.convert
  (:import [clojure.lang BigInt
                         IPersistentMap
                         Keyword
                         Ratio
                         Symbol]
           [java.time Instant]
           [java.util Date
                      List
                      Map
                      Set
                      UUID]
           [org.bson BsonArray
                     BsonBinary
                     BsonBoolean
                     BsonDateTime
                     BsonDecimal128
                     BsonDocument
                     BsonDouble
                     BsonInt32
                     BsonInt64
                     BsonNull
                     BsonObjectId
                     BsonString
                     BsonValue]
           [org.bson.conversions Bson]
           [org.bson.types Decimal128 ObjectId]))

(set! *warn-on-reflection* true)

(defprotocol ToKey
  (-to-key [this] "Convert value to BSON map key"))

(extend-protocol ToKey
  String
  (-to-key [this]
    this)
  Keyword
  (-to-key [this]
    (name this))
  Symbol
  (-to-key [this]
    (name this))
  Object
  (-to-key [this]
    (throw (ex-info "Not a valid BSON map key" {:key this}))))

(declare to-bson)

(defprotocol ToBSON
  (-to-bson [this] "Convert value to BSON"))

(extend-protocol ToBSON
  ;; Scalars
  nil
  (-to-bson [_]
    BsonNull/VALUE)
  ObjectId
  (-to-bson [this]
    (BsonObjectId. this))
  Boolean
  (-to-bson [this]
    (if this BsonBoolean/TRUE BsonBoolean/FALSE))
  Integer
  (-to-bson [this]
    (BsonInt32. this))
  Long
  (-to-bson [this]
    (BsonInt64. this))
  Double
  (-to-bson [this]
    (BsonDouble. this))
  BigDecimal
  (-to-bson [this]
    ;; Throws if value cannot be represented as Decimal128
    (BsonDecimal128. (Decimal128. this)))
  Character
  (-to-bson [this]
    (to-bson (str this)))
  String
  (-to-bson [this]
    (BsonString. this))
  Date
  (-to-bson [this]
    (BsonDateTime. (.getTime this)))
  Instant
  (-to-bson [this]
    (BsonDateTime. (.toEpochMilli this)))
  UUID
  (-to-bson [this]
    (BsonBinary. this))

  ;; java.util collections
  List
  (-to-bson [this]
    (BsonArray. ^List (mapv to-bson this)))
  Set
  (-to-bson [this]
    (BsonArray. ^List (mapv to-bson this)))
  Map
  (-to-bson [this]
    (let [doc (BsonDocument. (count this))]
      (doseq [[k v] this]
        (.put doc (-to-key k) (to-bson v)))
      doc))

  ;; Clojure stuff
  Keyword
  (-to-bson [this]
    (-to-bson (name this)))
  Symbol
  (-to-bson [this]
    (-to-bson (name this)))
  Ratio
  (-to-bson [this]
    (-to-bson (double this)))
  BigInt
  (-to-bson [this]
    (-to-bson (.toBigDecimal this)))
  IPersistentMap
  (-to-bson [this]
    (let [doc (BsonDocument. (count this))]
      (doseq [[k v] this]
        (let [k (-to-key k)]
          (if (and (= "_id" k) (string? v))
            (.put doc k (to-bson (ObjectId. ^String v)))
            (.put doc k (to-bson v)))))
      doc))

  ;; Error otherwise
  Object
  (-to-bson [this]
    (throw (ex-info "Cannot convert to BSON" {:value this
                                              :type  (type this)}))))

(defn to-bson
  "Convert value to BSON"
  ^BsonValue [object]
  (-to-bson object))

(defn map->bson
  "Convert a map to a `Bson`, which can produce a `BsonDocument`."
  ^Bson [m]
  {:pre [(map? m)]}
  (reify Bson
    (toBsonDocument [_this _documentClass _codecRegistry]
      ;; BsonDocument instances are mutable so always return a fresh
      ;; pristine instance in case the caller modifies it
      (to-bson m))))

(declare from-bson)

(defprotocol FromBSON
  (-from-bson [this opts] "Convert BSON to value"))

(extend-protocol FromBSON
  ;; Scalars
  BsonNull
  (-from-bson [_ _]
    nil)
  BsonObjectId
  (-from-bson [this _]
    (.toHexString (.getValue this)))
  BsonBoolean
  (-from-bson [this _]
    (.getValue this))
  BsonInt32
  (-from-bson [this _]
    (.getValue this))
  BsonInt64
  (-from-bson [this _]
    (.getValue this))
  BsonDouble
  (-from-bson [this _]
    (.getValue this))
  BsonDecimal128
  (-from-bson [this _]
    (.bigDecimalValue (.getValue this)))
  BsonString
  (-from-bson [this _]
    (.getValue this))
  BsonDateTime
  (-from-bson [this _]
    (Date. (.getValue this)))
  ;; Collections
  BsonArray
  (-from-bson [this opts]
    (->> (.getValues this)
         (mapv (partial from-bson opts))))
  BsonDocument
  (-from-bson [this {:keys [keywords?] :as opts}]
    (->> (.entrySet this)
         (mapcat (fn [[k v]]
                   [(if keywords? (keyword k) k)
                    (from-bson opts v)]))
         ;; BsonDocument contents are in a `java.util.LinkedHashMap`,
         ;; which retains the insertion order. Try to maintain that.
         (apply array-map))))

(defn from-bson
  ([bson]
   (from-bson {} bson))
  ([opts ^BsonValue bson]
   (-from-bson bson opts)))
