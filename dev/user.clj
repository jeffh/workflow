(ns user
  (:require [clojure.tools.namespace.repl :as repl]))

(repl/set-refresh-dirs
 "./workflow-core/src"
 "./workflow-core/test"
 "./workflow-jdbc-pg/src"
 "./workflow-jdbc-pg/test"
 "./workflow-kafka/src"
 "./workflow-kafka/test"
 "./workflow-tools-graphviz/src"
 "./workflow-tools-graphviz/test"
 "./workflow-tools-webviewer/src"
 "./workflow-tools-webviewer/test")

(defn refresh []
  (repl/refresh))

