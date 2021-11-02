(ns net.jeffhui.workflow.jdbc.pg
  (:require [net.jeffhui.workflow.protocol :as p]
            [net.jeffhui.workflow.api :as wf]
            [taoensso.nippy :as nippy]
            [clojure.set :as set]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [next.jdbc.connection :as connection])
  (:import com.zaxxer.hikari.HikariDataSource
           java.util.concurrent.Executors
           java.util.concurrent.ExecutorService
           java.util.UUID
           java.util.Date))

(defn- record
  ([f ds parameterized-query]
   (try
     (f ds parameterized-query)
     (catch org.postgresql.util.PSQLException pe
       (let [msg (.getServerErrorMessage pe)]
         (throw (ex-info "Failed to execute SQL"
                         {:sql (first parameterized-query)
                          :values (rest parameterized-query)
                          :pg-sql-state-error (some-> msg (.getSQLState))}
                         pe))))))
  ([f ds parameterized-query options]
   (try
     (f ds parameterized-query options)
     (catch org.postgresql.util.PSQLException pe
       (let [msg (.getServerErrorMessage pe)]
         (throw (ex-info "Failed to execute SQL"
                         {:sql (first parameterized-query)
                          :values (rest parameterized-query)
                          :pg-sql-state-error (some-> msg (.getSQLState))}
                         pe)))))))

(defn drop-tables!
  "Deletes all data. Please, I hope you know what you're doing."
  [ds]
  (record jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_executions"])
  (record jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_statemachines"])
  (record jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_scheduler_tasks"]))

(defn- ensure-statem-table [ds]
  (record jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_statemachines (
    id TEXT NOT NULL,
    version BIGINT NOT NULL,
	start_at TEXT NOT NULL,
	execution_mode TEXT NOT NULL,
	context BYTEA,
	states BYTEA,
	PRIMARY KEY(id, version)
  );"]))

(defn- ensure-executions-table [ds]
  (record jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_executions (
    id UUID NOT NULL,
	version BIGINT NOT NULL,
	state_machine_id TEXT NOT NULL,
	state_machine_version BIGINT NOT NULL,
	mode TEXT NOT NULL,
	state TEXT,
	pause_state TEXT,
  encoded BYTEA,
	enqueued_at BIGINT,
	started_at BIGINT,
	finished_at BIGINT,
	failed_at BIGINT,
	step_started_at BIGINT,
	step_ended_at BIGINT,
	user_started_at BIGINT,
	user_ended_at BIGINT,
	error BYTEA,
	comment TEXT,
	PRIMARY KEY (id, version)
  );
  CREATE INDEX IF NOT EXISTS workflow_executions_statem ON workflow_executions (state_machine_id, state_machine_version, enqueued_at);"]))

(defn- ensure-scheduler-table [ds]
  (record jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_scheduler_tasks (
    id TEXT PRIMARY KEY NOT NULL,
    execution_id UUID NOT NULL,
    start_after BIGINT NOT NULL,
    encoded BYTEA NOT NULL,
    response BYTEA
  );
  CREATE INDEX IF NOT EXISTS workflow_scheduler_tasks_start_after ON workflow_scheduler_tasks (start_after);
"]))

(defn- db->statem-txfm []
  (let [field->key {:id             :state-machine/id
                    :version        :state-machine/version
                    :start_at       :state-machine/start-at
                    :execution_mode :state-machine/execution-mode
                    :context        :state-machine/context
                    :states         :state-machine/states}]
    (comp
     (map #(set/rename-keys % field->key))
     (map #(select-keys % (vals field->key)))
     (map #(update % :state-machine/context nippy/fast-thaw))
     (map #(update % :state-machine/states nippy/fast-thaw)))))

(defn- db->execution-txfm []
  (map (fn [row]
         (merge (nippy/fast-thaw (:encoded row))
                (when-let [reply (:response row)]
                  {:task/response (nippy/fast-thaw reply)})))))

(defn- db->task-txfm []
  (map (comp nippy/fast-thaw :encoded)))

(defn- resolve-statem-version [conn state-machine-id version]
  (if (= :latest version)
    (:version (record jdbc/execute-one! conn ["SELECT MAX(version) as version FROM workflow_statemachines WHERE id = ?"
                                       state-machine-id]
                                 {:builder-fn rs/as-unqualified-maps}))
    version))

(defn- resolve-execution-version [conn execution-id version]
  (if (= :latest version)
    (:version (record jdbc/execute-one! conn ["SELECT MAX(version) as version FROM workflow_executions WHERE id = ?"
                                       execution-id]
                                 {:builder-fn rs/as-unqualified-maps}))
    version))

(defn- save-execution* [ds execution]
  (with-open [conn (jdbc/get-connection ds)]
    (try
      (record jdbc/execute! conn ["INSERT INTO workflow_executions (
      id,
      version,
      state_machine_id,
      state_machine_version,
      mode,
      state,
      pause_state,
      encoded,
      enqueued_at,
      started_at,
      finished_at,
      failed_at,
      step_started_at,
      step_ended_at,
      user_started_at,
      user_ended_at,
      error,
      comment
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                  (:execution/id execution)
                                  (:execution/version execution)
                                  (:execution/state-machine-id execution)
                                  (:execution/state-machine-version execution)
                                  (:execution/mode execution)
                                  (:execution/state execution)
                                  (:execution/pause-state execution)
                                  (nippy/fast-freeze execution)
                                  (:execution/enqueued-at execution)
                                  (:execution/started-at execution)
                                  (:execution/finished-at execution)
                                  (:execution/failed-at execution)
                                  (:execution/step-started-at execution)
                                  (:execution/step-ended-at execution)
                                  (:execution/user-started-at execution)
                                  (:execution/user-ended-at execution)
                                  (nippy/fast-freeze (:execution/error execution))
                                  (:execution/comment execution)])
      {:ok     true
       :entity execution}
      (catch clojure.lang.ExceptionInfo ei
        (let [expected-unique "23505"]
          (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
            (if (= expected-unique sql-state)
              {:ok false :error :version-conflict}
              (throw ei))
            (throw ei))))
      (catch org.postgresql.util.PSQLException pe
        (let [expected-unique "23505"]
          (if-let [msg (.getServerErrorMessage pe)]
            (if (= expected-unique (.getSQLState msg))
              {:ok false :error :version-conflict}
              (throw pe))
            (throw pe)))))))

(defrecord Persistence [db-spec ^javax.sql.DataSource ds ^ExecutorService pool]
  p/Connection
  (open* [this]
    (when ds (.close ^java.io.Closeable ds))
    (let [ds (connection/->pool HikariDataSource db-spec)]
      (ensure-statem-table ds)
      (ensure-executions-table ds)
      (assoc this :ds ds :pool (Executors/newSingleThreadExecutor (p/thread-factory (constantly "wf-jdbc-pg-persistence"))))))
  (close* [this]
    (when ds (.close ^java.io.Closeable ds))
    (when pool (.shutdown pool))
    (assoc this :ds nil :pool nil))
  p/StateMachinePersistence
  (fetch-statem [_ state-machine-id version]
    (with-open [conn (jdbc/get-connection db-spec)]
      (let [version (resolve-statem-version conn state-machine-id version)]
        (first
         (into []
               (db->statem-txfm)
               (jdbc/plan conn ["SELECT * FROM workflow_statemachines WHERE id = ? AND version = ?;" state-machine-id version]))))))
  (save-statem [_ state-machine options]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (record jdbc/execute! conn ["INSERT INTO workflow_statemachines (id, version, start_at, execution_mode, context, states) VALUES (?, ?, ?, ?, ?, ?)"
                                     (:state-machine/id state-machine)
                                     (:state-machine/version state-machine)
                                     (:state-machine/start-at state-machine)
                                     (:state-machine/execution-mode state-machine)
                                     (nippy/fast-freeze (:state-machine/context state-machine))
                                     (nippy/fast-freeze (:state-machine/states state-machine))])
         {:ok     true
          :entity state-machine}))))
  p/ExecutionPersistence
  (executions-for-statem [_ state-machine-id {:keys [version limit offset]}]
    (with-open [conn (jdbc/get-connection ds)]
      (let [version (resolve-statem-version conn state-machine-id version)]
        (into []
              (db->execution-txfm)
              (jdbc/plan
               conn
               (cond
                 limit ["SELECT e.* FROM workflow_executions e
INNER JOIN (
  SELECT id, MAX(version) as version FROM workflow_executions
  WHERE state_machine_id = ? AND state_machine_version = ?
  GROUP BY id
) as e2
ON e.id = e2.id AND e.version = e2.version
WHERE e.state_machine_id = ? AND e.state_machine_version = ?
ORDER BY e.enqueued_at DESC LIMIT ? OFFSET ?;"
                        state-machine-id version
                        state-machine-id version
                        limit (or offset 0)]
                 :else ["SELECT e.* FROM workflow_executions e
INNER JOIN (
  SELECT id, MAX(version) as version FROM workflow_executions
  WHERE state_machine_id = ? AND state_machine_version = ?
  GROUP BY id
) as e2
ON e.id = e2.id AND e.version = e2.version
WHERE e.state_machine_id = ? AND e.state_machine_version = ?
ORDER BY e.enqueued_at DESC;"
                        state-machine-id version
                        state-machine-id version]))))))
  (fetch-execution [_ execution-id version]
    (with-open [conn (jdbc/get-connection ds)]
      (let [version (resolve-execution-version conn execution-id version)]
        (first
         (into []
               (db->execution-txfm)
               (jdbc/plan conn ["SELECT * FROM workflow_executions WHERE id = ? AND version = ?;" execution-id version]))))))
  (fetch-execution-history [_ execution-id]
    (with-open [conn (jdbc/get-connection ds)]
      (into []
            (db->execution-txfm)
            (jdbc/plan conn ["SELECT * FROM workflow_executions WHERE id = ? ORDER BY version ASC;" execution-id]))))
  (save-execution [_ execution options]
    (.submit pool ^Callable (fn [] (save-execution* ds execution)))))

(defn make-persistence [db-spec]
  (->Persistence db-spec nil nil))

(defrecord SchedulerPersistence [db-spec ^javax.sql.DataSource ds ^ExecutorService pool]
  p/Connection
  (open* [this]
    (when ds (.close ^java.io.Closeable ds))
    (let [ds (connection/->pool HikariDataSource db-spec)]
      (ensure-scheduler-table ds)
      (assoc this :ds ds :pool (Executors/newSingleThreadExecutor (p/thread-factory (constantly "wf-jdbc-pg-scheduler-persistence"))))))
  (close* [this]
    (when ds (.close ^java.io.Closeable ds))
    (when pool (.shutdown pool))
    (assoc this :ds nil :pool nil))
  p/SchedulerPersistence
  (save-task [_ timestamp execution-id input]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (let [tid (str "task_" (UUID/randomUUID))]
           (try
             (record jdbc/execute! conn ["INSERT INTO workflow_scheduler_tasks (id, execution_id, start_after, encoded) VALUES (?, ?, ?, ?);"
                                         tid
                                         execution-id
                                         (.getTime ^Date timestamp)
                                         (nippy/fast-freeze #:task{:input        input
                                                                   :response     nil
                                                                   :start-after  timestamp
                                                                   :id           tid
                                                                   :execution-id execution-id})])
             {:task/id tid}
             (catch clojure.lang.ExceptionInfo ei
               (let [expected-unique "23505"]
                 (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
                   (if (= expected-unique sql-state)
                     {:error :version-conflict}
                     {:error (Throwable->map ei)})
                   {:error (Throwable->map ei)})))))))))
  (runnable-tasks [_ now]
    (with-open [conn (jdbc/get-connection db-spec)]
      (into []
            (db->task-txfm)
            (jdbc/plan
             conn
             ["SELECT encoded, response FROM workflow_scheduler_tasks WHERE start_after < ? ORDER BY start_after ASC"
              (.getTime ^Date now)]))))
  (complete-task [_ task-id reply]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (try
           (record jdbc/execute! conn ["UPDATE workflow_scheduler_tasks SET response = ? WHERE id = ? AND response IS NULL;"
                                       (nippy/fast-freeze reply)
                                       task-id])
           {:task/id task-id}
           (catch clojure.lang.ExceptionInfo ei
             (let [expected-unique "23505"]
               (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
                 (if (= expected-unique sql-state)
                   {:error :version-conflict}
                   {:error (Throwable->map ei)})
                 {:error (Throwable->map ei)}))))))))
  (delete-task [_ task-id]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (try
           (record jdbc/execute! conn ["DELETE FROM workflow_scheduler_tasks WHERE id = ?" task-id])
           (catch clojure.lang.ExceptionInfo ei
             {:error (Throwable->map ei)})))))))

(defn make-scheduler-persistence [db-spec]
  (->SchedulerPersistence db-spec nil nil))

