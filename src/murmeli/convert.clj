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
                      Set]
           [org.bson BsonArray
                     BsonBoolean
                     BsonDateTime
                     BsonDecimal128
                     BsonDocument
                     BsonDouble
                     BsonInt32
                     BsonInt64
                     BsonNull
                     BsonString
                     BsonValue]
           [org.bson.types Decimal128]))

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
    (BsonDecimal128. (Decimal128. this)))
  String
  (-to-bson [this]
    (BsonString. this))
  Date
  (-to-bson [this]
    (BsonDateTime. (.getTime this)))
  Instant
  (-to-bson [this]
    (BsonDateTime. (.toEpochMilli this)))

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
        (.put doc (-to-key k) (to-bson v)))
      doc))

  ;; Error otherwise
  Object
  (-to-bson [this]
    (throw (ex-info "Cannot convert to BSON" {:value this
                                              :type  (type this)}))))

(defn to-bson
  "Convert value to BSON"
  ^BsonValue [object]
  (if (some? object)
    (-to-bson object)
    BsonNull/VALUE))
