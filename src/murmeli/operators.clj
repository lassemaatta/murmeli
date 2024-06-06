(ns murmeli.operators
  "From https://www.mongodb.com/docs/manual/reference/operator/ (7.0)")

;;; Query and Projection Operators

;; Comparison

(def ^:const $eq "Matches values that are equal to a specified value." "$eq")
(def ^:const $gt "Matches values that are greater than a specified value." "$gt")
(def ^:const $gte "Matches values that are greater than or equal to a specified value." "$gte")
(def ^:const $in "Matches any of the values specified in an array." "$in")
(def ^:const $lt "Matches values that are less than a specified value." "$lt")
(def ^:const $lte "Matches values that are less than or equal to a specified value." "$lte")
(def ^:const $ne "Matches all values that are not equal to a specified value." "$ne")
(def ^:const $nin "Matches none of the values specified in an array." "$nin")

;; Logical

(def ^:const $and "Joins query clauses with a logical AND returns all documents that match the conditions of both clauses." "$and")
(def ^:const $not "Inverts the effect of a query expression and returns documents that do not match the query expression." "$not")
(def ^:const $nor "Joins query clauses with a logical NOR returns all documents that fail to match both clauses." "$nor")
(def ^:const $or "Joins query clauses with a logical OR returns all documents that match the conditions of either clause." "$or")

;; Element

(def ^:const $exists "Matches documents that have the specified field." "$exists")
(def ^:const $type "Selects documents if a field is of the specified type." "$type")

;; Evaluation

(def ^:const $expr "Allows use of aggregation expressions within the query language." "$expr")
(def ^:const $jsonSchema "Validate documents against the given JSON Schema." "$jsonSchema")
(def ^:const $mod "Performs a modulo operation on the value of a field and selects documents with a specified result." "$mod")
(def ^:const $regex "Selects documents where values match a specified regular expression." "$regex")
(def ^:const $text "Performs text search." "$text")
(def ^:const $where "Matches documents that satisfy a JavaScript expression." "$where")

;; Geospatial

(def ^:const $geoIntersects "Selects geometries that intersect with a GeoJSON geometry. The 2dsphere index supports `$geoIntersects`." "$geoIntersects")
(def ^:const $geoWithin "Selects geometries within a bounding GeoJSON geometry. The 2dsphere and 2d indexes support `$geoWithin`." "$geoWithin")
(def ^:const $near "Returns geospatial objects in proximity to a point. Requires a geospatial index. The 2dsphere and 2d indexes support `$near`." "$near")
(def ^:const $nearSphere "Returns geospatial objects in proximity to a point on a sphere. Requires a geospatial index. The 2dsphere and 2d indexes support `$nearSphere`." "$nearSphere")

;; Array

(def ^:const $all "Matches arrays that contain all elements specified in the query." "$all")
(def ^:const $elemMatch "Selects documents if element in the array field matches all the specified `$elemMatch` conditions." "$elemMatch")
(def ^:const $size "Selects documents if the array field is a specified size." "$size")

;; Bitwise

(def ^:const $bitsAllClear "Matches numeric or binary values in which a set of bit positions all have a value of 0." "$bitsAllClear")
(def ^:const $bitsAllSet "Matches numeric or binary values in which a set of bit positions all have a value of 1." "$bitsAllSet")
(def ^:const $bitsAnyClear "Matches numeric or binary values in which any bit from a set of bit positions has a value of 0." "$bitsAnyClear")
(def ^:const $bitsAnySet "Matches numeric or binary values in which any bit from a set of bit positions has a value of 1." "$bitsAnySet")

;;; Projection Operators

(def ^:const $ "Projects the first element in an array that matches the query condition." "$")
(def ^:const $elemMatch "Projects the first element in an array that matches the specified $elemMatch condition." "$elemMatch")
(def ^:const $meta "Projects the document's score assigned during $text operation." "$meta")
(def ^:const $slice "Limits the number of elements projected from an array. Supports skip and limit slices." "$slice")

;;; Miscellaneous Operators

(def ^:const $comment "Adds a comment to a query predicate." "$comment")
(def ^:const $rand "Generates a random float between 0 and 1." "$rand")
