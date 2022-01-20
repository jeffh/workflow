(ns net.jeffhui.workflow.kafka-test
  (:require [net.jeffhui.workflow.kafka :as kafka]
            [net.jeffhui.workflow.memory :as mem]
            [net.jeffhui.workflow.jdbc.pg :as pg]
            [net.jeffhui.workflow.contracts :as contracts]
            [net.jeffhui.workflow.impl.kafka.messaging :as messaging]
            [next.jdbc :as jdbc]
            [clojure.test :refer [deftest]])
  (:import java.util.UUID))

(def config {"bootstrap.servers" "localhost:9092"})

(def pg-url "jdbc:postgresql://localhost:5432/workflow_test?user=postgres&password=password")
(defn- make-pg-scheduler-persistence [conn-url]
  (fn creator []
    (let [db-spec {:jdbcUrl conn-url}]
      (try
        (pg/drop-tables! (jdbc/get-datasource db-spec))
        (catch Exception _ nil))
      (pg/make-scheduler-persistence db-spec))))

(defn make-test-scheduler [make-scheduler-persistence]
  (let [adm (messaging/->admin config)]
    (try
      (try
        @(messaging/delete-topics adm ["test-workflow-executions" "test-workflow-responses"])
        (catch Exception e nil))
      (Thread/sleep 5000)
      (finally
        (messaging/close! adm)))
    (kafka/make-scheduler (str "test-consumer-id-" (UUID/randomUUID))
                          (make-scheduler-persistence)
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
                                                     :replication-factor 1}})))

(deftest scheduler-contract-mem
  (contracts/scheduler "Kafka Scheduler + Memory Persistence" (partial make-test-scheduler mem/make-scheduler-persistence)))

(deftest scheduler-contract-pg
  (contracts/scheduler "Kafka Scheduler + Postgres Persistence" (partial make-test-scheduler (make-pg-scheduler-persistence pg-url))))

(deftest sch-mem-e2e
  (contracts/effects "Scheduler + Memory"
                     (fn []
                       (let [statem (mem/make-statem-persistence)]
                         {:statem statem
                          :execution (mem/make-execution-persistence)
                          :scheduler (make-test-scheduler mem/make-scheduler-persistence)}))))
