(defproject murmeli "0.1.0-SNAPSHOT"
  :description "A simple clojure wrapper for the MongoDB Java driver"
  :url "https://github.com/lassemaatta/murmeli"
  :license {:name "European Union Public Licence v. 1.2"
            :url  "https://joinup.ec.europa.eu/collection/eupl/eupl-text-eupl-12"}
  :dependencies [[org.clojure/clojure "1.12.0"]
                 [org.clojure/tools.logging "1.3.0"]
                 [org.mongodb/mongodb-driver-sync "5.2.1"]
                 [prismatic/schema "1.4.1"]]
  :target-path "target/%s"
  :profiles {:dev           {:dependencies   [;; Sources (incl. deps)
                                              [org.mongodb/mongodb-driver-sync "5.2.1" :classifier "sources"]
                                              [org.mongodb/bson "5.2.1" :classifier "sources"]
                                              [org.mongodb/mongodb-driver-core "5.2.1" :classifier "sources"]
                                              ;; Testcontainers for mongodb
                                              [clj-test-containers "0.7.4"]
                                              [org.testcontainers/mongodb "1.17.6"]
                                              ;; Generative testing with spec
                                              [org.clojure/test.check "1.1.1"]
                                              ;; Matcher for tests
                                              [nubank/matcher-combinators "3.9.1"]
                                              ;; Logging
                                              [org.slf4j/slf4j-api "2.0.16"]
                                              [ch.qos.logback/logback-classic "1.5.12"]]
                             :resource-paths ["test-resources"]
                             :jvm-opts       ["-Dclojure.tools.logging.factory=clojure.tools.logging.impl/slf4j-factory"]}
             :repl          {:jvm-opts ["-Dmurmeli.repl=true"]}
             :gen-doc-tests {:test-paths   ["target/test-doc-blocks/test"]
                             :dependencies [[com.github.lread/test-doc-blocks "1.1.20"]]}}
  :aliases  {"run-doc-tests" ^{:doc "Generate, then run, tests from doc code blocks"}
             ["with-profile" "+gen-doc-tests" "do"
              ["run" "-m" "lread.test-doc-blocks" "gen-tests" "--platform" "clj" "README.md"]
              ["test"]]})
