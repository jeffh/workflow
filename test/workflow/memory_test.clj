(ns workflow.memory-test
  (:require [clojure.test :refer [deftest]]
            [workflow.memory :as mem]
            [workflow.contracts :as contracts]))

(deftest memory-persistenc-contract
  (contracts/statem-persistence "Memory Statem Persistence" mem/make-statem-persistence))

(deftest execution-persistence-contract
  (contracts/execution-persistence "Memory Execution Persistence"
                                   (comp mem/make-execution-persistence mem/make-statem-persistence)))
