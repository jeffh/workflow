(ns workflow.memory-test
  (:require [clojure.test :refer [deftest]]
            [workflow.memory :as mem]
            [workflow.contracts :as contracts]))

(deftest statem-persistence-contract
  (contracts/statem-persistence "Memory Statem Persistence" mem/make-statem-persistence))

(deftest execution-persistence-contract
  (contracts/execution-persistence "Memory Execution Persistence" (comp mem/make-execution-persistence mem/make-statem-persistence)))

(deftest scheduler-contract
  (contracts/scheduler "Memory Scheduler" mem/make-scheduler))
