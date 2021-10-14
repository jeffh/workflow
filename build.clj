(ns build
  (:require [clojure.tools.build.api :as b]
            [org.corfield.build :as bb]))

(def lib 'net.jeffhui/workflow)
(def version (format "0.1.%s" (b/git-count-revs nil)))

(defn run-tests [opts]
  (let [{:keys [exit]} (b/process {:command-args ["clojure" "-X:test"]})]
    (when-not (zero? exit)
      (throw (ex-info "Test failed" {}))))
  opts)

(defn clean [opts]
  (-> opts
      (assoc :lib lib :version version)
      (bb/clean)))

(defn ci [opts]
  (-> opts
      (assoc :lib lib :version version)
      (run-tests)
      (bb/clean)
      (bb/jar)))

(defn uberjar [opts]
  (-> opts
      (assoc :lib lib :version version)
      (run-tests)
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
