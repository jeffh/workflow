(ns net.jeffhui.workflow.memory-test
  (:require [clojure.test :refer [deftest]]
            [net.jeffhui.workflow.memory :as mem]
            [net.jeffhui.workflow.contracts :as contracts]))

(deftest statem-persistence-contract
  (contracts/statem-persistence "Memory Statem Persistence" mem/make-statem-persistence))

(deftest execution-persistence-contract
  (contracts/execution-persistence "Memory Execution Persistence" mem/make-execution-persistence))

(deftest scheduler-persistence-contract
  (contracts/scheduler-persistence "Memory Scheduler Persistence" mem/make-scheduler-persistence))

(deftest scheduler-contract
  (contracts/scheduler "Memory Scheduler" mem/make-scheduler))

(deftest e2e
  (contracts/effects "Memory Execution"
                     (fn []
                       {:statem    (mem/make-statem-persistence)
                        :execution (mem/make-execution-persistence)
                        :scheduler (mem/make-scheduler)})))

