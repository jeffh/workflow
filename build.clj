(ns build
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'net.jeffhui/workflow)
(def version (format "0.1.%s" (b/git-count-revs nil)))

(defn run-tests [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/run-tests)))

(defn clean [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/clean)))

(defn ci [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/run-tests)
      (bb/clean)
      (bb/jar)))

(defn uberjar [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/run-tests)
      (bb/clean)
      (bb/uber)))

(defn install [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/install)))

(defn clojars-deploy [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/deploy)))
