(ns net.jeffhui.workflow.jdbc.pg-test
  (:require [clojure.test :refer [deftest]]
            [net.jeffhui.workflow.jdbc.pg :as pg]
            [net.jeffhui.workflow.contracts :as contracts]
            [next.jdbc :as jdbc]
            next.jdbc.date-time))

(defn- make-test-persistence []
  (let [db-spec {:jdbcUrl "jdbc:postgresql://localhost:5432/workflow_test?user=postgres&password=password"}]
    (try
      (pg/drop-tables! (jdbc/get-datasource db-spec))
      (catch Exception _ nil))
    (pg/make-persistence db-spec)))

(deftest statem-persistence-contract
  (contracts/statem-persistence "Postgres Statem Persistence" make-test-persistence))

(deftest execution-persistence-contract
  (contracts/execution-persistence "Postgres Execution Persistence" make-test-persistence))

(defn- make-test-scheduler-persistence []
  (let [db-spec {:jdbcUrl "jdbc:postgresql://localhost:5432/workflow_test?user=postgres&password=password"}]
    (try
      (pg/drop-tables! (jdbc/get-datasource db-spec))
      (catch Exception _ nil))
    (pg/make-scheduler-persistence db-spec)))

(deftest scheduler-persistence-contract
  (contracts/scheduler-persistence "Postgres Scheduler Persistence" make-test-scheduler-persistence))
