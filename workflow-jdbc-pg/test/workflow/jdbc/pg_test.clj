(ns workflow.jdbc.pg-test
  (:require [clojure.test :refer [deftest]]
            [workflow.jdbc.pg :as pg]
            [workflow.contracts :as contracts]
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

