(defproject workflow "0.1.0-SNAPSHOT"
  :description "Data programs"
  :url "https://github.com/jeffh/workflow"
  :license {:name "EPL-2.0 WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/core.async "1.3.618"]
                 [borkdude/sci "0.2.6"]
                 [metosin/jsonista "0.3.3"]
                 [org.apache.kafka/kafka-clients "3.0.0"]
                 [org.apache.kafka/kafka-streams "3.0.0"]
                 [com.taoensso/nippy "3.1.1"]
                 [com.github.seancorfield/next.jdbc "1.2.724"]
                 [com.zaxxer/HikariCP "5.0.0"]
                 [org.postgresql/postgresql "42.2.24"]]
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  :repl-options {:init-ns workflow.api})
