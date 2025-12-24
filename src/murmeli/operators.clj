(ns murmeli.operators
  "From https://www.mongodb.com/docs/manual/reference/mql/ (8.0)"
  (:require [clojure.string :as str]))

;;; Aggregation stages (https://www.mongodb.com/docs/manual/reference/mql/aggregation-stages/)

(def ^:const $addFields "https://www.mongodb.com/docs/manual/reference/operator/aggregation/addFields/" "$addFields")
(def ^:const $bucket "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucket/" "$bucket")
(def ^:const $bucketAuto "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/" "$bucketAuto")
(def ^:const $changeStream "https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStream/" "$changeStream")
(def ^:const ^{:added "6.0.9"} $changeStreamSplitLargeEvent "https://www.mongodb.com/docs/manual/reference/operator/aggregation/changeStreamSplitLargeEvent/" "$changeStreamSplitLargeEvent")
(def ^:const $collStats "https://www.mongodb.com/docs/manual/reference/operator/aggregation/collStats/" "$collStats")
(def ^:const $count
  "https://www.mongodb.com/docs/manual/reference/operator/aggregation/count/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/count-accumulator/ "
  "$count")
(def ^:const $currentOp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/currentOp/" "$currentOp")
(def ^:const ^{:added "5.1"} $densify "https://www.mongodb.com/docs/manual/reference/operator/aggregation/densify/" "$densify")
(def ^:const ^{:added "6.0"} $documents "https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/" "$documents")
(def ^:const $facet "https://www.mongodb.com/docs/manual/reference/operator/aggregation/facet/" "$facet")
(def ^:const ^{:added "5.3"} $fill "https://www.mongodb.com/docs/manual/reference/operator/aggregation/fill/" "$fill")
(def ^:const $geoNear "https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/" "$geoNear")
(def ^:const ^{:added "5.1"} $graphLookup "https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/" "$graphLookup")
(def ^:const $group "https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/" "$group")
(def ^:const $indexStats "https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexStats/" "$indexStats")
(def ^:const $limit "https://www.mongodb.com/docs/manual/reference/operator/aggregation/limit/" "$limit")
(def ^:const ^{:added "8.1"} $listClusterCatalog "https://www.mongodb.com/docs/manual/reference/operator/aggregation/listClusterCatalog/" "$listClusterCatalog")
(def ^:const $listLocalSessions "https://www.mongodb.com/docs/manual/reference/operator/aggregation/listLocalSessions/" "$listLocalSessions")
(def ^:const $listSampledQueries "https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSampledQueries/" "$listSampledQueries")
(def ^:const ^{:added "6.0.7"} $listSearchIndexes "https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSearchIndexes/" "$listSearchIndexes")
(def ^:const $listSessions "https://www.mongodb.com/docs/manual/reference/operator/aggregation/listSessions/" "$listSessions")
(def ^:const $lookup "https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/" "$lookup")
(def ^:const $match "https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/" "$match")
(def ^:const $merge "https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/" "$merge")
(def ^:const $out "https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/" "$out")
(def ^:const $planCacheStats "https://www.mongodb.com/docs/manual/reference/operator/aggregation/planCacheStats/" "$planCacheStats")
(def ^:const $project "https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/" "$project")
(def ^:const ^{:added "8.0"} $querySettings "https://www.mongodb.com/docs/manual/reference/operator/aggregation/querySettings/" "$querySettings")
(def ^:const $queryStats "https://www.mongodb.com/docs/manual/reference/operator/aggregation/queryStats/" "$queryStats")
(def ^:const ^{:added "8.2 preview"} $rankFusion "https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/" "$rankFusion")
(def ^:const $redact "https://www.mongodb.com/docs/manual/reference/operator/aggregation/redact/" "$redact")
(def ^:const $replaceRoot "https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/" "$replaceRoot")
(def ^:const $replaceWith "https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceWith/" "$replaceWith")
(def ^:const $sample "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/" "$sample")
(def ^:const ^{:added "8.2"} $score "https://www.mongodb.com/docs/manual/reference/operator/aggregation/score/" "$score")
(def ^:const ^{:added "8.2 preview"} $scoreFusion "https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/" "$scoreFusion")
(def ^:const $search "https://www.mongodb.com/docs/manual/reference/operator/aggregation/search/" "$search")
(def ^:const $searchMeta "https://www.mongodb.com/docs/manual/reference/operator/aggregation/searchMeta/" "$searchMeta")
(def ^:const ^{:added "5.0"} $setWindowFields "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setWindowFields/" "$setWindowFields")
(def ^:const ^{:added "6.0.3"} $sharedDataDistribution "https://www.mongodb.com/docs/manual/reference/operator/aggregation/shardedDataDistribution/" "$sharedDataDistribution")
(def ^:const $skip "https://www.mongodb.com/docs/manual/reference/operator/aggregation/skip/" "$skip")
(def ^:const $sortByCount "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortByCount/" "$sortByCount")
(def ^:const $unionWith "https://www.mongodb.com/docs/manual/reference/operator/aggregation/unionWith/" "$unionWith")
(def ^:const $unwind "https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/" "$unwind")
(def ^:const $vectorSearch "https://www.mongodb.com/docs/manual/reference/operator/aggregation/vectorSearch/" "$vectorSearch")

;;; Query Predicates (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/)

;; Array (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/arrays/)

(def ^:const $all "https://www.mongodb.com/docs/manual/reference/operator/query/all/" "$all")
(def ^:const $elemMatch
  "https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/
  https://www.mongodb.com/docs/manual/reference/operator/projection/elemMatch/" "$elemMatch")
(def ^:const $size
  "https://www.mongodb.com/docs/manual/reference/operator/query/size/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/"
  "$size")

;; Bitwise (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/bitwise/)

(def ^:const $bitsAllClear "https://www.mongodb.com/docs/manual/reference/operator/query/bitsAllClear/" "$bitsAllClear")
(def ^:const $bitsAllSet "https://www.mongodb.com/docs/manual/reference/operator/query/bitsAllSet/" "$bitsAllSet")
(def ^:const $bitsAnyClear "https://www.mongodb.com/docs/manual/reference/operator/query/bitsAnyClear/" "$bitsAnyClear")
(def ^:const $bitsAnySet "https://www.mongodb.com/docs/manual/reference/operator/query/bitsAnySet/" "$bitsAnySet")

;; Comparison (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/comparison/)

(def ^:const $eq
  "https://www.mongodb.com/docs/manual/reference/operator/query/eq/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/eq/"
  "$eq")
(def ^:const $gt
  "https://www.mongodb.com/docs/manual/reference/operator/query/gt/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/gt/"
  "$gt")
(def ^:const $gte
  "https://www.mongodb.com/docs/manual/reference/operator/query/gte/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/gte/"
  "$gte")
(def ^:const $in
  "https://www.mongodb.com/docs/manual/reference/operator/query/in/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/"
  "$in")
(def ^:const $lt
  "https://www.mongodb.com/docs/manual/reference/operator/query/lt/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/lt/"
  "$lt")
(def ^:const $lte
  "https://www.mongodb.com/docs/manual/reference/operator/query/lte/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/lte/"
  "$lte")
(def ^:const $ne
  "https://www.mongodb.com/docs/manual/reference/operator/query/ne/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/ne/"
  "$ne")
(def ^:const $nin "https://www.mongodb.com/docs/manual/reference/operator/query/nin/" "$nin")

;; Data type (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/data-type/)

(def ^:const $exists "https://www.mongodb.com/docs/manual/reference/operator/query/exists/" "$exists")
(def ^:const $type
  "https://www.mongodb.com/docs/manual/reference/operator/query/type/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/"
  "$type")

;; Geospatial (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/geospatial/)

(def ^:const $box "https://www.mongodb.com/docs/manual/reference/operator/query/box/" "$box")
(def ^:const $center "https://www.mongodb.com/docs/manual/reference/operator/query/center/" "$center")
(def ^:const $centerSphere "https://www.mongodb.com/docs/manual/reference/operator/query/centerSphere/" "$centerSphere")
(def ^:const $geometry "https://www.mongodb.com/docs/manual/reference/operator/query/geometry/" "$geometry")
(def ^:const $geoIntersects "https://www.mongodb.com/docs/manual/reference/operator/query/geoIntersects/" "$geoIntersects")
(def ^:const $geoWithin "https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/" "$geoWithin")
(def ^:const $near "https://www.mongodb.com/docs/manual/reference/operator/query/near/" "$near")
(def ^:const $nearSphere "https://www.mongodb.com/docs/manual/reference/operator/query/nearSphere/" "$nearSphere")
(def ^:const $minDistance "https://www.mongodb.com/docs/manual/reference/operator/query/minDistance/" "$minDistance")
(def ^:const $maxDistance "https://www.mongodb.com/docs/manual/reference/operator/query/maxDistance/" "$maxDistance")
(def ^:const $polygon "https://www.mongodb.com/docs/manual/reference/operator/query/polygon/" "$polygon")

;; Logical (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/logical/)

(def ^:const $and "https://www.mongodb.com/docs/manual/reference/operator/query/and/" "$and")
(def ^:const $not "https://www.mongodb.com/docs/manual/reference/operator/query/not/" "$not")
(def ^:const $nor "https://www.mongodb.com/docs/manual/reference/operator/query/nor/" "$nor")
(def ^:const $or "https://www.mongodb.com/docs/manual/reference/operator/query/or/" "$or")

;; Miscellaneous (https://www.mongodb.com/docs/manual/reference/mql/query-predicates/misc/)

(def ^:const $expr "https://www.mongodb.com/docs/manual/reference/operator/query/expr/" "$expr")
(def ^:const $jsonSchema "https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/" "$jsonSchema")
(def ^:const $mod
  "https://www.mongodb.com/docs/manual/reference/operator/query/mod/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/mod/"
  "$mod")
(def ^:const $regex "https://www.mongodb.com/docs/manual/reference/operator/query/regex/" "$regex")
(def ^:const $options "https://www.mongodb.com/docs/manual/reference/operator/query/regex/#mongodb-query-op.-options" "$options")
(def ^:const $text "https://www.mongodb.com/docs/manual/reference/operator/query/text/" "$text")
(def ^:const ^{:deprecated "8.0"} $where "https://www.mongodb.com/docs/manual/reference/operator/query/where/" "$where")

;;; Expressions (https://www.mongodb.com/docs/manual/reference/mql/expressions/)

(def ^:const $abs "https://www.mongodb.com/docs/manual/reference/operator/aggregation/abs/" "$abs")
(def ^:const $acos "https://www.mongodb.com/docs/manual/reference/operator/aggregation/acos/" "$acos")
(def ^:const $acosh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/acosh/" "$acosh")
(def ^:const $add "https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/" "$add")
(def ^:const $allElementsTrue "https://www.mongodb.com/docs/manual/reference/operator/aggregation/allElementsTrue/" "$allElementsTrue")
(def ^:const $anyElementTrue "https://www.mongodb.com/docs/manual/reference/operator/aggregation/anyElementTrue/" "$anyElementTrue")
(def ^:const $arrayElementAt "https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/" "$arrayElementAt")
(def ^:const $arrayToObject "https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayToObject/" "$arrayToObject")
(def ^:const $asin "https://www.mongodb.com/docs/manual/reference/operator/aggregation/asin/" "$asin")
(def ^:const $asinh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/asinh/" "$asinh")
(def ^:const $atan "https://www.mongodb.com/docs/manual/reference/operator/aggregation/atan/" "$atan")
(def ^:const $atan2 "https://www.mongodb.com/docs/manual/reference/operator/aggregation/atan2/" "$atan2")
(def ^:const $atanh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/atanh/" "$atanh")
(def ^:const $binarySize "https://www.mongodb.com/docs/manual/reference/operator/aggregation/binarySize/" "$binarySize")
(def ^:const ^{:added "6.3"} $bitAnd "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitAnd/" "$bitAnd")
(def ^:const ^{:added "6.3"} $bitNot "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitNot/" "$bitNot")
(def ^:const ^{:added "6.3"} $bitOr "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitOr/" "$bitOr")
(def ^:const ^{:added "6.3"} $bitXor "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitXor/" "$bitXor")
(def ^:const $bsonSize "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bsonSize/" "$bsonSize")
(def ^:const $ceil "https://www.mongodb.com/docs/manual/reference/operator/aggregation/ceil/" "$ceil")
(def ^:const $cmp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/cmp/" "$cmp")
(def ^:const ^:deprecated $comment "https://www.mongodb.com/docs/v4.4/reference/operator/query/comment/" "$comment")
(def ^:const $concat "https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/" "$concat")
(def ^:const $concatArrays "https://www.mongodb.com/docs/manual/reference/operator/aggregation/concatArrays/" "$concatArrays")
(def ^:const $cond "https://www.mongodb.com/docs/manual/reference/operator/aggregation/cond/" "$cond")
(def ^:const $convert "https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/" "$convert")
(def ^:const $cos "https://www.mongodb.com/docs/manual/reference/operator/aggregation/cos/" "$cos")
(def ^:const $cosh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/cosh/" "$cosh")
(def ^:const ^{:added "5.0"} $covariancePop "https://www.mongodb.com/docs/manual/reference/operator/aggregation/covariancePop/" "$covariancePop")
(def ^:const ^{:added "5.0"} $covarianceSamp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/covarianceSamp/" "$covarianceSamp")
(def ^:const ^{:added "5.0"} $dateAdd "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateAdd/" "$dateAdd")
(def ^:const ^{:added "5.0"} $dateDiff "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/" "$dateDiff")
(def ^:const $dateFromParts "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromParts/" "$dateFromParts")
(def ^:const $dateFromString "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/" "$dateFromString")
(def ^:const ^{:added "5.0"} $dateSubtract "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateSubtract/" "$dateSubtract")
(def ^:const $dateToParts "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToParts/" "$dateToParts")
(def ^:const $dateToString "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/" "$dateToString")
(def ^:const ^{:added "5.0"} $dateTrunc "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateTrunc/" "$dateTrunc")
(def ^:const $dayOfMonth "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfMonth/" "$dayOfMonth")
(def ^:const $dayOfWeek "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfWeek/" "$dayOfWeek")
(def ^:const $dayOfYear "https://www.mongodb.com/docs/manual/reference/operator/aggregation/dayOfYear/" "$dayOfYear")
(def ^:const $degreesToRadians "https://www.mongodb.com/docs/manual/reference/operator/aggregation/degreesToRadians/" "$degreesToRadians")
(def ^:const ^{:added "5.0"} $denseRank "https://www.mongodb.com/docs/manual/reference/operator/aggregation/denseRank/" "$denseRank")
(def ^:const ^{:added "5.0"} $derivative "https://www.mongodb.com/docs/manual/reference/operator/aggregation/derivative/" "$derivative")
(def ^:const $divide "https://www.mongodb.com/docs/manual/reference/operator/aggregation/divide/" "$divide")
(def ^:const ^{:added "5.0"} $documentNumber "https://www.mongodb.com/docs/manual/reference/operator/aggregation/documentNumber/" "$documentNumber")
(def ^:const ^{:added "8.2"} $encStrContains "https://www.mongodb.com/docs/manual/reference/operator/aggregation/encStrContains/" "$encStrContains")
(def ^:const ^{:added "8.2"} $encStrEndsWith "https://www.mongodb.com/docs/manual/reference/operator/aggregation/encStrEndsWith/" "$encStrEndsWith")
(def ^:const ^{:added "8.2"} $encStrNormalizedEq "https://www.mongodb.com/docs/manual/reference/operator/aggregation/encStrNormalizedEq/" "$encStrNormalizedEq")
(def ^:const ^{:added "8.2"} $encStrStartsWith "https://www.mongodb.com/docs/manual/reference/operator/aggregation/encStrStartsWith/" "$encStrStartsWith")
(def ^:const $exp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/exp/" "$exp")
(def ^:const ^{:added "5.0"} $expMovingAvg "https://www.mongodb.com/docs/manual/reference/operator/aggregation/expMovingAvg/" "$expMovingAvg")
(def ^:const $filter "https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/" "$filter")
(def ^:const $floor "https://www.mongodb.com/docs/manual/reference/operator/aggregation/floor/" "$floor")
(def ^:const ^{:deprecated "8.0"} $function "https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/" "$function")
(def ^:const ^{:added "5.0"} $getField "https://www.mongodb.com/docs/manual/reference/operator/aggregation/getField/" "$getField")
(def ^:const $hour "https://www.mongodb.com/docs/manual/reference/operator/aggregation/hour/" "$hour")
(def ^:const $ifNull "https://www.mongodb.com/docs/manual/reference/operator/aggregation/ifNull/" "$ifNull")
(def ^:const $indexOfArray "https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfArray/" "$indexOfArray")
(def ^:const $indexOfBytes "https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfBytes/" "$indexOfBytes")
(def ^:const $indexOfCP "https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfCP/" "$indexOfCP")
(def ^:const ^{:added "5.0"} $integral "https://www.mongodb.com/docs/manual/reference/operator/aggregation/integral/" "$integral")
(def ^:const $isArray "https://www.mongodb.com/docs/manual/reference/operator/aggregation/isArray/" "$isArray")
(def ^:const $isNumber "https://www.mongodb.com/docs/manual/reference/operator/aggregation/isNumber/" "$isNumber")
(def ^:const $isoDayOfWeek "https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoDayOfWeek/" "$isoDayOfWeek")
(def ^:const $isoWeek "https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeek/" "$isoWeek")
(def ^:const $isoWeekYear "https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeekYear/" "$isoWeekYear")
(def ^:const $let "https://www.mongodb.com/docs/manual/reference/operator/aggregation/let/" "$let")
(def ^:const ^{:added "5.3"} $linearFill "https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill/" "$linearFill")
(def ^:const $literal "https://www.mongodb.com/docs/manual/reference/operator/aggregation/literal/" "$literal")
(def ^:const $ln "https://www.mongodb.com/docs/manual/reference/operator/aggregation/ln/" "$ln")
(def ^:const ^{:added "5.2"} $locf "https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf/" "$locf")
(def ^:const $log "https://www.mongodb.com/docs/manual/reference/operator/aggregation/log/" "$log")
(def ^:const $log10 "https://www.mongodb.com/docs/manual/reference/operator/aggregation/log10/" "$log10")
(def ^:const $ltrim "https://www.mongodb.com/docs/manual/reference/operator/aggregation/ltrim/" "$ltrim")
(def ^:const $map "https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/" "$map")
(def ^:const ^{:added "5.2"} $maxN-array-element "https://www.mongodb.com/docs/manual/reference/operator/aggregation/maxN-array-element/" "$maxN-array-element")
(def ^:const $meta "https://www.mongodb.com/docs/manual/reference/operator/aggregation/meta/" "$meta")
(def ^:const ^{:added "5.2"} $minN-array-element "https://www.mongodb.com/docs/manual/reference/operator/aggregation/minN-array-element/" "$minN-array-element")
(def ^:const ^{:added "8.2"} $minMaxScaler "https://www.mongodb.com/docs/manual/reference/operator/aggregation/minMaxScaler/" "$minMaxScaler")
(def ^:const $millisecond "https://www.mongodb.com/docs/manual/reference/operator/aggregation/millisecond/" "$millisecond")
(def ^:const $minute "https://www.mongodb.com/docs/manual/reference/operator/aggregation/minute/" "$minute")
(def ^:const $month "https://www.mongodb.com/docs/manual/reference/operator/aggregation/month/" "$month")
(def ^:const $multiply "https://www.mongodb.com/docs/manual/reference/operator/aggregation/multiply/" "$multiply")
(def ^:const $objectToArray "https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/" "$objectToArray")
(def ^:const $pow "https://www.mongodb.com/docs/manual/reference/operator/aggregation/pow/" "$pow")
(def ^:const $radiansToDegrees "https://www.mongodb.com/docs/manual/reference/operator/aggregation/radiansToDegrees/" "$radiansToDegrees")
(def ^:const $rand "https://www.mongodb.com/docs/manual/reference/operator/aggregation/rand/" "$rand")
(def ^:const $range "https://www.mongodb.com/docs/manual/reference/operator/aggregation/range/" "$range")
(def ^:const ^{:added "5.0"} $rank "https://www.mongodb.com/docs/manual/reference/operator/aggregation/rank/" "$rank")
(def ^:const $reduce "https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/" "$reduce")
(def ^:const $regexFind "https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFind/" "$regexFind")
(def ^:const $regexFindAll "https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFindAll/" "$regexFindAll")
(def ^:const $regexMatch "https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexMatch/" "$regexMatch")
(def ^:const $replaceOne "https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceOne/" "$replaceOne")
(def ^:const $replaceAll "https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceAll/" "$replaceAll")
(def ^:const $reverseArray "https://www.mongodb.com/docs/manual/reference/operator/aggregation/reverseArray/" "$reverseArray")
(def ^:const $round "https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/" "$round")
(def ^:const $rtrim "https://www.mongodb.com/docs/manual/reference/operator/aggregation/rtrim/" "$rtrim")
(def ^:const $sampleRate "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sampleRate/" "$sampleRate")
(def ^:const $second "https://www.mongodb.com/docs/manual/reference/operator/aggregation/second/" "$second")
(def ^:const $setDifference "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setDifference/" "$setDifference")
(def ^:const $setEquals "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setEquals/" "$setEquals")
(def ^:const ^{:added "5.0"} $setField "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/" "$setField")
(def ^:const $setIntersection "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIntersection/" "$setIntersection")
(def ^:const $setIsSubset "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIsSubset/" "$setIsSubset")
(def ^:const $setUnion "https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/" "$setUnion")
(def ^:const ^{:added "5.0"} $shift "https://www.mongodb.com/docs/manual/reference/operator/aggregation/shift/" "$shift")
(def ^:const $sigmoid "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sigmoid/" "$sigmoid")
(def ^:const $sin "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sin/" "$sin")
(def ^:const $sinh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sinh/" "$sinh")
(def ^:const ^{:added "5.2"} $sortArray "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortArray/" "$sortArray")
(def ^:const $split "https://www.mongodb.com/docs/manual/reference/operator/aggregation/split/" "$split")
(def ^:const $sqrt "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sqrt/" "$sqrt")
(def ^:const $strcasecmp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/strcasecmp/" "$strcasecmp")
(def ^:const $strLenBytes "https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenBytes/" "$strLenBytes")
(def ^:const $strLenCP "https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenCP/" "$strLenCP")
(def ^:const $substr "https://www.mongodb.com/docs/manual/reference/operator/aggregation/substr/" "$substr")
(def ^:const $substrBytes "https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrBytes/" "$substrBytes")
(def ^:const $substrCP "https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrCP/" "$substrCP")
(def ^:const $subtract "https://www.mongodb.com/docs/manual/reference/operator/aggregation/subtract/" "$subtract")
(def ^:const $switch "https://www.mongodb.com/docs/manual/reference/operator/aggregation/switch/" "$switch")
(def ^:const $tan "https://www.mongodb.com/docs/manual/reference/operator/aggregation/tan/" "$tan")
(def ^:const $tanh "https://www.mongodb.com/docs/manual/reference/operator/aggregation/tanh/" "$tanh")
(def ^:const $toBool "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toBool/" "$toBool")
(def ^:const $toDate "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/" "$toDate")
(def ^:const $toDecimal "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDecimal/" "$toDecimal")
(def ^:const $toDouble "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDouble/" "$toDouble")
(def ^:const $toHashedIndexKey "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toHashedIndexKey/" "$toHashedIndexKey")
(def ^:const $toInt "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toInt/" "$toInt")
(def ^:const $toLong "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLong/" "$toLong")
(def ^:const $toObjectId "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toObjectId/" "$toObjectId")
(def ^:const $toString "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toString/" "$toString")
(def ^:const $toLower "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLower/" "$toLower")
(def ^:const $toUpper "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toUpper/" "$toUpper")
(def ^:const ^{:added "8.0"} $toUUID "https://www.mongodb.com/docs/manual/reference/operator/aggregation/toUUID/" "$toUUID")
(def ^:const ^{:added "5.1"} $tsIncrement "https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsIncrement/" "$tsIncrement")
(def ^:const ^{:added "5.1"} $tsSecond "https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsSecond/" "$tsSecond")
(def ^:const $trim "https://www.mongodb.com/docs/manual/reference/operator/aggregation/trim/" "$trim")
(def ^:const $trunc "https://www.mongodb.com/docs/manual/reference/operator/aggregation/trunc/" "$trunc")
(def ^:const ^{:added "5.0"} $unsetField "https://www.mongodb.com/docs/manual/reference/operator/aggregation/unsetField/" "$unsetField")
(def ^:const $week "https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/" "$week")
(def ^:const $year "https://www.mongodb.com/docs/manual/reference/operator/aggregation/year/" "$year")
(def ^:const $zip "https://www.mongodb.com/docs/manual/reference/operator/aggregation/zip/" "$zip")

;;; Projection Operators (https://www.mongodb.com/docs/manual/reference/mql/projection/)

(def ^:const $
  "https://www.mongodb.com/docs/manual/reference/operator/projection/positional/
  https://www.mongodb.com/docs/manual/reference/operator/update/positional/"
  "$")
(def ^:const $slice
  "https://www.mongodb.com/docs/manual/reference/operator/projection/slice/
  https://www.mongodb.com/docs/manual/reference/operator/update/slice/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/"
  "$slice")

;;; Accumulators (https://www.mongodb.com/docs/manual/reference/mql/accumulators/)

(def ^:const ^{:deprecated "8.0"} $accumulator "https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/" "$accumulator")
(def ^:const $avg "https://www.mongodb.com/docs/manual/reference/operator/aggregation/avg/" "$avg")
(def ^:const ^{:added "5.2"} $bottom "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottom/" "$bottom")
(def ^:const ^{:added "5.2"} $bottomN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/bottomN/" "$bottomN")
(def ^:const $first "https://www.mongodb.com/docs/manual/reference/operator/aggregation/first/" "$first")
(def ^:const ^{:added "5.2"} $firstN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/firstN/" "$firstN")
(def ^:const $last "https://www.mongodb.com/docs/manual/reference/operator/aggregation/last/" "$last")
(def ^:const ^{:added "5.2"} $lastN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/lastN/" "$lastN")
(def ^:const ^{:added "5.2"} $maxN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/maxN/" "$maxN")
(def ^:const ^{:added "7.0"} $median "https://www.mongodb.com/docs/manual/reference/operator/aggregation/median/" "$median")
(def ^:const $mergeObjects "https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/" "$mergeObjects")
(def ^:const ^{:added "5.2"} $minN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/minN/" "$minN")
(def ^:const ^{:added "7.0"} $percentile "https://www.mongodb.com/docs/manual/reference/operator/aggregation/percentile/" "$percentile")
(def ^:const $stdDevPop "https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevPop/" "$stdDevPop")
(def ^:const $stdDevSamp "https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevSamp/" "$stdDevSamp")
(def ^:const $sum "https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/" "$sum")
(def ^:const ^{:added "5.2"} $top "https://www.mongodb.com/docs/manual/reference/operator/aggregation/top/" "$top")
(def ^:const ^{:added "5.2"} $topN "https://www.mongodb.com/docs/manual/reference/operator/aggregation/topN/" "$topN")

;;; Update Operators (https://www.mongodb.com/docs/manual/reference/mql/update/)

;; Array (https://www.mongodb.com/docs/manual/reference/operator/update-array/)

(def ^:const $addToSet
  "https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/addToSet/"
  "$addToSet")
(def ^:const $pop "https://www.mongodb.com/docs/manual/reference/operator/update/pop/" "$pop")
(def ^:const $pull "https://www.mongodb.com/docs/manual/reference/operator/update/pull/" "$pull")
(def ^:const $push
  "https://www.mongodb.com/docs/manual/reference/operator/update/push/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/push/"
  "$push")
(def ^:const $pullAll "https://www.mongodb.com/docs/manual/reference/operator/update/pullAll/" "$pullAll")
(def ^:const $each "https://www.mongodb.com/docs/manual/reference/operator/update/each/" "$each")
(def ^:const $position "https://www.mongodb.com/docs/manual/reference/operator/update/position/" "$position")
(def ^:const $sort
  "https://www.mongodb.com/docs/manual/reference/operator/update/sort/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/  "
  "$sort")

;; Bitwise (https://www.mongodb.com/docs/manual/reference/operator/update-bitwise/)

(def ^:const $bit "https://www.mongodb.com/docs/manual/reference/operator/update/bit/" "$bit")

;; Fields (https://www.mongodb.com/docs/manual/reference/operator/update-field/)

(def ^:const $currentDate "https://www.mongodb.com/docs/manual/reference/operator/update/currentdate/" "$currentDate")
(def ^:const $inc "https://www.mongodb.com/docs/manual/reference/operator/update/inc/" "$inc")
(def ^:const $min
  "https://www.mongodb.com/docs/manual/reference/operator/update/min/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/min/"
  "$min")
(def ^:const $max
  "https://www.mongodb.com/docs/manual/reference/operator/update/max/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/"
  "$max")
(def ^:const $mul "https://www.mongodb.com/docs/manual/reference/operator/update/mul/" "$mul")
(def ^:const $rename "https://www.mongodb.com/docs/manual/reference/operator/update/rename/" "$rename")
(def ^:const $set
  "https://www.mongodb.com/docs/manual/reference/operator/update/set/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/set/" "$set")
(def ^:const $setOnInsert "https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/" "$setOnInsert")
(def ^:const $unset
  "https://www.mongodb.com/docs/manual/reference/operator/update/unset/
  https://www.mongodb.com/docs/manual/reference/operator/aggregation/unset/"
  "$unset")

;;; API

(def operators
  "The set of supported operators"
  (->> 'murmeli.operators
       ns-publics
       keys
       (map name)
       (filter (fn [s] (str/starts-with? s "$")))
       set))

(defn operator?
  "Check if given string matches a known operator."
  [s]
  (contains? operators s))
