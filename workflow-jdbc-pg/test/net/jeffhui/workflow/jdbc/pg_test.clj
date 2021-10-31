(ns net.jeffhui.workflow.jdbc.pg-test
  (:require [clojure.test :refer [deftest]]
            [net.jeffhui.workflow.jdbc.pg :as pg]
            [net.jeffhui.workflow.contracts :as contracts]
            [next.jdbc :as jdbc]
            next.jdbc.date-time))

(def pg-url "jdbc:postgresql://localhost:5432/workflow_test?user=postgres&password=password")
(def crdb-url "jdbc:postgresql://localhost:26257/workflow_test?user=root&sslmode=disable")

(defn- make-test-persistence [conn-url]
  (fn creator []
    (let [db-spec {:jdbcUrl conn-url}]
      (try
        (pg/drop-tables! (jdbc/get-datasource db-spec))
        (catch Exception _ nil))
      (pg/make-persistence db-spec))))

(defn- make-test-scheduler-persistence [conn-url]
  (fn creator []
    (let [db-spec {:jdbcUrl conn-url}]
      (try
        (pg/drop-tables! (jdbc/get-datasource db-spec))
        (catch Exception _ nil))
      (pg/make-scheduler-persistence db-spec))))

(deftest pg-statem-persistence-contract
  (contracts/statem-persistence "Postgres Statem Persistence" (make-test-persistence pg-url)))

(deftest pg-execution-persistence-contract
  (contracts/execution-persistence "Postgres Execution Persistence" (make-test-persistence pg-url)))

(deftest pg-scheduler-persistence-contract
  (contracts/scheduler-persistence "Postgres Scheduler Persistence" (make-test-scheduler-persistence pg-url)))

(deftest crdb-statem-persistence-contract
  (contracts/statem-persistence "CockroachDB Statem Persistence" (make-test-persistence crdb-url)))

(deftest crdb-execution-persistence-contract
  (contracts/execution-persistence "CockroachDB Execution Persistence" (make-test-persistence crdb-url)))

(deftest crdb-scheduler-persistence-contract
  (contracts/scheduler-persistence "CockroachDB Scheduler Persistence" (make-test-scheduler-persistence crdb-url)))
