(ns murmeli.cursor
  "`MongoIterable` implements `java.lang.Iterable`, so we can give it directly
  to `transduce` or `reduce` as the `coll`. However, the cursor/iterator it
  returns _must_ be closed (according to the docs). To handle this, we can
  extend `coll-reduce` for `MongoIterable` and control when the cursor is closed."
  (:require [clojure.core.protocols :as protocols])
  (:import [com.mongodb.client MongoIterable]))

(set! *warn-on-reflection* true)

(defn- iterator->iterable
  "Silly trick to convert an `Iterator` back to an `Iterable`"
  [it]
  (reify Iterable
    (iterator [_]
      it)))

(defn- cursor-reduce
  "Like `coll-reduce` for `Iterable`s, but has special handling for closing cursors"
  ([^MongoIterable iterable f]
   (with-open [it (.cursor iterable)]
     (protocols/coll-reduce (iterator->iterable it) f)))
  ([^MongoIterable iterable f v]
   (with-open [it (.cursor iterable)]
     (protocols/coll-reduce (iterator->iterable it) f v))))

(extend-protocol protocols/CollReduce
  MongoIterable
  (coll-reduce
    ([coll f] (cursor-reduce coll f))
    ([coll f v] (cursor-reduce coll f v))))
