(ns net.jeffhui.workflow.memory-test
  (:require [clojure.test :refer [deftest]]
            [net.jeffhui.workflow.memory :as mem]
            [net.jeffhui.workflow.contracts :as contracts]))

(deftest statem-persistence-contract
  (contracts/statem-persistence "Memory Statem Persistence" mem/make-statem-persistence))

(deftest execution-persistence-contract
  (contracts/execution-persistence "Memory Execution Persistence" (comp mem/make-execution-persistence mem/make-statem-persistence)))

(deftest scheduler-contract
  (contracts/scheduler "Memory Scheduler" mem/make-scheduler))

(deftest e2e
  (contracts/effects "Memory Execution"
                     (fn []
                       (let [statem (mem/make-statem-persistence)]
                         {:statem    statem
                          :execution (mem/make-execution-persistence statem)
                          :scheduler (mem/make-scheduler)}))))

