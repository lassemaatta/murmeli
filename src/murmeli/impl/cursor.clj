(ns murmeli.impl.cursor
  (:import [clojure.lang IReduceInit]
           [com.mongodb.client MongoIterable]))

(set! *warn-on-reflection* true)

(defn ->reducible
  "Produce a _reducible_ ([IReduceInit](https://github.com/clojure/clojure/blob/master/src/jvm/clojure/lang/IReduceInit.java))
  from a [MongoIterable](https://mongodb.github.io/mongo-java-driver/5.3/apidocs/mongodb-driver-sync/com/mongodb/client/MongoIterable.html).

  Guarantees that any cursors are closed after reducing."
  [^MongoIterable iterable]
  (reify
    ;; TODO: we might want to add an interface which allows the
    ;; consumer to directly access the original `iterable`
    IReduceInit
    (reduce [_ f start]
      ;; "An application should ensure that a cursor is closed in all circumstances,
      ;; e.g. using a try-with-resources statement"
      (with-open [it (.cursor iterable)]
        (loop [acc start]
          (if-not (.hasNext it)
            acc
            (let [acc (f acc (.next it))]
              (if (reduced? acc)
                (deref acc)
                (recur acc)))))))))
