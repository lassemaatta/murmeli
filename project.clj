(defproject murmeli "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.3"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.mongodb/mongodb-driver-sync "5.1.0"]]
  :target-path "target/%s"
  :profiles {:dev {:dependencies   [;; Sources
                                    [org.mongodb/mongodb-driver-sync "5.1.0" :classifier "sources"]
                                    ;; Testcontainers for mongodb
                                    [clj-test-containers "0.7.4"]
                                    ;; Logging
                                    [org.slf4j/slf4j-api "2.0.4"]
                                    [ch.qos.logback/logback-classic "1.3.5"]]
                   :resource-paths ["test-resources"]
                   :jvm-opts       ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/slf4j-factory"]}})
