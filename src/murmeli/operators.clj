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
(def ^:const $meta "Projects the document's score assigned during $text operation." "$meta")
(def ^:const $slice "Limits the number of elements projected from an array. Supports skip and limit slices." "$slice")

;;; Miscellaneous Operators

(def ^:const $comment "Adds a comment to a query predicate." "$comment")
(def ^:const $rand "Generates a random float between 0 and 1." "$rand")

;;; Update Operators

;; Fields

(def ^:const $currentDate "Sets the value of a field to current date, either as a Date or a Timestamp." "$currentDate")
(def ^:const $inc "Increments the value of the field by the specified amount." "$inc")
(def ^:const $min "Only updates the field if the specified value is less than the existing field value." "$min")
(def ^:const $max "Only updates the field if the specified value is greater than the existing field value." "$max")
(def ^:const $mul "Multiplies the value of the field by the specified amount." "$mul")
(def ^:const $rename "Renames a field." "$rename")
(def ^:const $set "Sets the value of a field in a document." "$set")
(def ^:const $setOnInsert "Sets the value of a field if an update results in an insert of a document. Has no effect on update operations that modify existing documents." "$setOnInsert")
(def ^:const $unset "Removes the specified field from a document." "$unset")

;; Array

(def ^:const $addToSet "Adds elements to an array only if they do not already exist in the set." "$addToSet")
(def ^:const $pop "Removes the first or last item of an array." "$pop")
(def ^:const $pull "Removes all array elements that match a specified query." "$pull")
(def ^:const $push "Adds an item to an array." "$push")
(def ^:const $pullAll "Removes all matching values from an array." "$pullAll")

;; Modifiers

(def ^:const $each "Modifies the `$push` and `$addToSet` operators to append multiple items for array updates." "$each")
(def ^:const $position "Modifies the `$push` operator to specify the position in the array to add elements." "$position")
(def ^:const $sort "Modifies the $push operator to reorder documents stored in an array." "$sort")
(def ^:const $bit "Performs bitwise AND, OR, and XOR updates of integer values." "$bit")
