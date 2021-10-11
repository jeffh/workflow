(ns workflow.kafka-test
  (:require [workflow.kafka :as kafka]
            [workflow.memory :as mem]
            [workflow.contracts :as contracts]
            [workflow.impl.kafka.messaging :as messaging]
            [clojure.test :refer [deftest]])
  (:import java.util.UUID))

(def config {"bootstrap.servers" "localhost:9092"})

(deftest scheduler-contract
  (contracts/scheduler
   "Kafka Scheduler + Memory Persistence"
   #(let [adm (messaging/->admin config)]
      (try
        (try
          @(messaging/delete-topics adm ["test-workflow-executions" "test-workflow-responses"])
          (catch Exception e nil))
        (Thread/sleep 5000)
        (finally
          (messaging/close! adm)))
      (kafka/make-scheduler (str "test-consumer-id-" (UUID/randomUUID))
                            (mem/make-scheduler-persistence)
                            {:status-poll-interval-ms 100
                             :name                    (str (UUID/randomUUID))
                             ;; TODO(jeff): make this work
                             ;; :producer-fn             messaging/->mock-producer
                             ;; :consumer-fn             messaging/->mock-consumer
                             :producer-options        config
                             :consumer-options        config
                             :execution-topic-config  {:name               "test-workflow-executions"
                                                       :num-partitions     10
                                                       :replication-factor 1}
                             :response-topic-config   {:name               "test-workflow-responses"
                                                       :num-partitions     1
                                                       :replication-factor 1}}))))

