(ns murmeli.operators
  "From https://www.mongodb.com/docs/manual/reference/operator/ (7.0)"
  (:require [clojure.string :as str]))

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
(def ^:const $options "Options for `$regex`" "$options")
(def ^:const $text "Performs text search." "$text")
(def ^:const $where "Matches documents that satisfy a JavaScript expression." "$where")

;; Geospatial

(def ^:const $geometry "Specifies a GeoJSON geometry." "$geometry")
(def ^:const $geoIntersects "Selects geometries that intersect with a GeoJSON geometry. The 2dsphere index supports `$geoIntersects`." "$geoIntersects")
(def ^:const $geoWithin "Selects geometries within a bounding GeoJSON geometry. The 2dsphere and 2d indexes support `$geoWithin`." "$geoWithin")
(def ^:const $near "Returns geospatial objects in proximity to a point. Requires a geospatial index. The 2dsphere and 2d indexes support `$near`." "$near")
(def ^:const $nearSphere "Returns geospatial objects in proximity to a point on a sphere. Requires a geospatial index. The 2dsphere and 2d indexes support `$nearSphere`." "$nearSphere")
(def ^:const $minDistance "Filters the results of a geospatial `$near` or `$nearSphere` query to those documents that are at least the specified distance from the center point." "$minDistance")
(def ^:const $maxDistance "Constrains the results of a geospatial `$near` or `$nearSphere` query to the specified distance." "$maxDistance")

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

;;; Aggregation

;; Stages

(def ^:const $addFields "Add new fields to documents." "$addFields")
(def ^:const $bucket "" "$bucket")
(def ^:const $bucketAuto "" "$bucketAuto")
(def ^:const $changeStream "" "$changeStream")
(def ^:const $changeStreamSplitLargeEvent "" "$changeStreamSplitLargeEvent")
(def ^:const $collStats "" "$collStats")
(def ^:const $count "" "$count")
(def ^:const $currentOp "" "$currentOp")
(def ^:const $densify "" "$densify")
(def ^:const $documents "" "$documents")
(def ^:const $facet "" "$facet")
(def ^:const $fill "" "$fill")
(def ^:const $geoNear "" "$geoNear")
(def ^:const $graphLookup "" "$graphLookup")
(def ^:const $group "" "$group")
(def ^:const $indexStats "" "$indexStats")
(def ^:const $limit "" "$limit")
(def ^:const $listLocalSessions "" "$listLocalSessions")
(def ^:const $listSampledQueries "" "$listSampledQueries")
(def ^:const $listSearchIndexes "" "$listSearchIndexes")
(def ^:const $listSessions "" "$listSessions")
(def ^:const $lookup "" "$lookup")
(def ^:const $match "Filters documents based on a specified query predicate." "$match")
(def ^:const $merge "" "$merge")
(def ^:const $out "" "$out")
(def ^:const $planCacheStats "" "$planCacheStats")
(def ^:const $project "Passes along the documents with the requested fields to the next stage in the pipeline." "$project")
(def ^:const $querySettings "" "$querySettings")
(def ^:const $queryStats "" "$queryStats")
(def ^:const $redact "" "$redact")
(def ^:const $replaceRoot "" "$replaceRoot")
(def ^:const $replaceWith "" "$replaceWith")
(def ^:const $sample "" "$sample")
(def ^:const $search "" "$search")
(def ^:const $searchMeta "" "$searchMeta")
(def ^:const $setWindowFields "" "$setWindowFields")
(def ^:const $sharedDataDistribution "" "$sharedDataDistribution")
(def ^:const $skip "" "$skip")
(def ^:const $sortByCount "" "$sortByCount")
(def ^:const $unionWith "" "$unionWith")
(def ^:const $unwind "" "$unwind")
(def ^:const $vectorSearch "" "$vectorSearch")

;; Operators

(def ^:const $abs "" "$abs")
(def ^:const $accumulator "" "$accumulator")
(def ^:const $acos "" "$acos")
(def ^:const $acosh "" "$acosh")
(def ^:const $add "" "$add")
(def ^:const $allElementsTrue "" "$allElementsTrue")
(def ^:const $anyElementTrue "" "$anyElementTrue")
(def ^:const $arrayElementAt "" "$arrayElementAt")
(def ^:const $arrayToObject "" "$arrayToObject")
(def ^:const $asin "" "$asin")
(def ^:const $asinh "" "$asinh")
(def ^:const $atan "" "$atan")
(def ^:const $atan2 "" "$atan2")
(def ^:const $atanh "" "$atanh")
(def ^:const $avg "" "$avg")
(def ^:const $binarySize "" "$binarySize")
(def ^:const $bitAnd "" "$bitAnd")
(def ^:const $bitNot "" "$bitNot")
(def ^:const $bitOr "" "$bitOr")
(def ^:const $bitXor "" "$bitXor")
(def ^:const $bottom "" "$bottom")
(def ^:const $bottomN "" "$bottomN")
(def ^:const $bsonSize "" "$bsonSize")
(def ^:const $ceil "" "$ceil")
(def ^:const $cmp "" "$cmp")
(def ^:const $concat "" "$concat")
(def ^:const $concatArrays "" "$concatArrays")
(def ^:const $cond "" "$cond")
(def ^:const $convert "" "$convert")
(def ^:const $cos "" "$cos")
(def ^:const $cosh "" "$cosh")
(def ^:const $count-accumulator "" "$count-accumulator")
(def ^:const $covariancePop "" "$covariancePop")
(def ^:const $covarianceSamp "" "$covarianceSamp")
(def ^:const $dateAdd "" "$dateAdd")
(def ^:const $dateDiff "" "$dateDiff")
(def ^:const $dateFromParts "" "$dateFromParts")
(def ^:const $dateFromString "" "$dateFromString")
(def ^:const $dateSubtract "" "$dateSubtract")
(def ^:const $dateToParts "" "$dateToParts")
(def ^:const $dateToString "" "$dateToString")
(def ^:const $dateTrunc "" "$dateTrunc")
(def ^:const $dayOfMonth "" "$dayOfMonth")
(def ^:const $dayOfWeek "" "$dayOfWeek")
(def ^:const $dayOfYear "" "$dayOfYear")
(def ^:const $degreesToRadians "" "$degreesToRadians")
(def ^:const $denseRank "" "$denseRank")
(def ^:const $derivative "" "$derivative")
(def ^:const $divide "" "$divide")
(def ^:const $documentNumber "" "$documentNumber")
(def ^:const $exp "" "$exp")
(def ^:const $expMovingAvg "" "$expMovingAvg")
(def ^:const $filter "" "$filter")
(def ^:const $first "" "$first")
(def ^:const $firstN "" "$firstN")
(def ^:const $floor "" "$floor")
(def ^:const $function "" "$function")
(def ^:const $getField "" "$getField")
(def ^:const $hour "" "$hour")
(def ^:const $ifNull "Evaluates input expressions for null values" "$ifNull")
(def ^:const $indexOfArray "" "$indexOfArray")
(def ^:const $indexOfBytes "" "$indexOfBytes")
(def ^:const $indexOfCP "" "$indexOfCP")
(def ^:const $integral "" "$integral")
(def ^:const $isArray "" "$isArray")
(def ^:const $isNumber "" "$isNumber")
(def ^:const $isoDayOfWeek "" "$isoDayOfWeek")
(def ^:const $isoWeek "" "$isoWeek")
(def ^:const $isoWeekYear "" "$isoWeekYear")
(def ^:const $last "" "$last")
(def ^:const $lastN "" "$lastN")
(def ^:const $let "" "$let")
(def ^:const $linearFill "" "$linearFill")
(def ^:const $literal "" "$literal")
(def ^:const $ln "" "$ln")
(def ^:const $locf "" "$locf")
(def ^:const $log "" "$log")
(def ^:const $log10 "" "$log10")
(def ^:const $ltrim "" "$ltrim")
(def ^:const $map "" "$map")
(def ^:const $maxN "" "$maxN")
(def ^:const $maxN-array-element "" "$maxN-array-element")
(def ^:const $median "" "$median")
(def ^:const $mergeObjects "" "$mergeObjects")
(def ^:const $minN "" "$minN")
(def ^:const $minN-array-element "" "$minN-array-element")
(def ^:const $millisecond "" "$millisecond")
(def ^:const $minute "" "$minute")
(def ^:const $month "" "$month")
(def ^:const $multiply "" "$multiply")
(def ^:const $objectToArray "Converts a document to an array." "$objectToArray")
(def ^:const $percentile "" "$percentile")
(def ^:const $pow "" "$pow")
(def ^:const $radiansToDegrees "" "$radiansToDegrees")
(def ^:const $range "" "$range")
(def ^:const $rank "" "$rank")
(def ^:const $reduce "" "$reduce")
(def ^:const $regexFind "" "$regexFind")
(def ^:const $regexFindAll "" "$regexFindAll")
(def ^:const $regexMatch "" "$regexMatch")
(def ^:const $replaceOne "" "$replaceOne")
(def ^:const $replaceAll "" "$replaceAll")
(def ^:const $reverseArray "" "$reverseArray")
(def ^:const $round "" "$round")
(def ^:const $rtrim "" "$rtrim")
(def ^:const $sampleRate "" "$sampleRate")
(def ^:const $second "" "$second")
(def ^:const $setDifference "" "$setDifference")
(def ^:const $setEquals "" "$setEquals")
(def ^:const $setField "" "$setField")
(def ^:const $setIntersection "" "$setIntersection")
(def ^:const $setIsSubset "" "$setIsSubset")
(def ^:const $setUnion "" "$setUnion")
(def ^:const $shift "" "$shift")
(def ^:const $sin "" "$sin")
(def ^:const $sinh "" "$sinh")
(def ^:const $sortArray "" "$sortArray")
(def ^:const $split "" "$split")
(def ^:const $sqrt "" "$sqrt")
(def ^:const $stdDevPop "" "$stdDevPop")
(def ^:const $stdDevSamp "" "$stdDevSamp")
(def ^:const $strcasecmp "" "$strcasecmp")
(def ^:const $strLenBytes "" "$strLenBytes")
(def ^:const $strLenCP "" "$strLenCP")
(def ^:const $substr "" "$substr")
(def ^:const $substrBytes "" "$substrBytes")
(def ^:const $substrCP "" "$substrCP")
(def ^:const $subtract "" "$subtract")
(def ^:const $sum "" "$sum")
(def ^:const $switch "" "$switch")
(def ^:const $tan "" "$tan")
(def ^:const $tanh "" "$tanh")
(def ^:const $toBool "" "$toBool")
(def ^:const $toDate "" "$toDate")
(def ^:const $toDecimal "" "$toDecimal")
(def ^:const $toDouble "" "$toDouble")
(def ^:const $toHashedIndexKey "" "$toHashedIndexKey")
(def ^:const $toInt "" "$toInt")
(def ^:const $toLong "" "$toLong")
(def ^:const $toObjectId "" "$toObjectId")
(def ^:const $top "" "$top")
(def ^:const $topN "" "$topN")
(def ^:const $toString "" "$toString")
(def ^:const $toLower "" "$toLower")
(def ^:const $toUpper "" "$toUpper")
(def ^:const $toUUID "" "$toUUID")
(def ^:const $tsIncrement "" "$tsIncrement")
(def ^:const $tsSecond "" "$tsSecond")
(def ^:const $trim "" "$trim")
(def ^:const $trunc "" "$trunc")
(def ^:const $unsetField "" "$unsetField")
(def ^:const $week "" "$week")
(def ^:const $year "" "$year")
(def ^:const $zip "" "$zip")

;;; API

(def operators (->> 'murmeli.operators
                    ns-publics
                    keys
                    (map name)
                    (filter (fn [s] (str/starts-with? s "$")))
                    set))

(defn operator?
  [s]
  (contains? operators s))
