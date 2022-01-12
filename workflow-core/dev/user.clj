(ns user
  (:require [clojure.tools.namespace.repl :as repl]))

(repl/set-refresh-dirs
 "./src"
 "./test")

(defn refresh []
  (repl/refresh))


