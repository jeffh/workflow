(ns net.jeffhui.workflow.jdbc.pg
  (:require [net.jeffhui.workflow.protocol :as p]
            [net.jeffhui.workflow.api :as wf]
            [net.jeffhui.workflow.tracer :as tracer]
            [taoensso.nippy :as nippy]
            [clojure.set :as set]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]
            [next.jdbc.connection :as connection]
            [net.jeffhui.workflow.api :as api]
            [clojure.string :as string]
            [clojure.walk :as walk])
  (:import com.zaxxer.hikari.HikariDataSource
           java.util.concurrent.Executors
           java.util.concurrent.ExecutorService
           java.util.UUID
           java.util.Date))

(def ^:private tracer (tracer/get-tracer "net.jeffhui.workflow.jdbc.pg"))

;; NOTE(jeff): defined to provider easier debugging for serialization issues
(defn- freeze [v]
  (->> v
       (walk/postwalk (fn [form]
                        (cond
                          (delay? form) (pr-str @form)
                          :else form)))
       nippy/fast-freeze))

(defn- thaw [v]
  (nippy/fast-thaw v))

(defn- record
  ([trace-name f ds parameterized-query]
  (tracer/with-span [sp "run-sql"]
    (tracer/set-attr-str sp "sql" (first parameterized-query))
    (tap> {::type trace-name ::sql parameterized-query})
    (f ds parameterized-query)
    (catch org.postgresql.util.PSQLException pe
      (tracer/record-exception sp pe)
      (tap> {::type trace-name ::sql parameterized-query ::exception pe})
      (let [msg (.getServerErrorMessage pe)]
        (throw (ex-info "Failed to execute SQL"
                        {:sql                (first parameterized-query)
                         :values             (rest parameterized-query)
                         :pg-sql-state-error (some-> msg (.getSQLState))}
                        pe))))))
  ([trace-name f ds parameterized-query options]
   (tracer/with-span [sp "run-sql"]
     (tracer/set-attr-str sp "sql" (first parameterized-query))
     (tap> {::type trace-name ::sql parameterized-query})
     (f ds parameterized-query options)
     (catch org.postgresql.util.PSQLException pe
       (tracer/record-exception sp pe)
       (tap> {::type trace-name ::sql parameterized-query ::exception pe})
       (let [msg (.getServerErrorMessage pe)]
         (throw (ex-info "Failed to execute SQL"
                         {:sql                (first parameterized-query)
                          :values             (rest parameterized-query)
                          :pg-sql-state-error (some-> msg (.getSQLState))}
                         pe)))))))

(defn drop-tables!
  "Deletes all data. Please, I hope you know what you're doing."
  [ds]
  (record :drop-tables! jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_executions"])
  (record :drop-tables! jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_statemachines"])
  (record :drop-tables! jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_scheduler_tasks"]))

(defn- ensure-statem-table [ds]
  (record :ensure-statem-table jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_statemachines (
    id TEXT NOT NULL,
    version BIGINT NOT NULL,
	start_at TEXT NOT NULL,
	execution_mode TEXT NOT NULL,
	context BYTEA,
	states BYTEA,
	PRIMARY KEY(id, version)
  );"]))

(defn- ensure-executions-table [ds]
  (record :ensure-executions-table jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_executions (
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
  (record :ensure-scheduler-table jdbc/execute! ds ["CREATE TABLE IF NOT EXISTS workflow_scheduler_tasks (
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
     (map #(update % :state-machine/context thaw))
     (map #(update % :state-machine/states thaw)))))

(defn- db->execution-txfm []
  (map (fn [row]
         (merge (thaw (:encoded row))
                (when-let [reply (:response row)]
                  {:task/response (thaw reply)})))))

(defn- db->task-txfm []
  (map (comp thaw :encoded)))

(defn- resolve-statem-version [conn state-machine-id version]
  (if (= :latest version)
    (:version (record :resolve-statem-version jdbc/execute-one! conn ["SELECT MAX(version) as version FROM workflow_statemachines WHERE id = ?"
                                       state-machine-id]
                                 {:builder-fn rs/as-unqualified-maps}))
    version))

(defn- resolve-execution-version [conn execution-id version]
  (if (= :latest version)
    (:version (record :resolve-execution-version jdbc/execute-one! conn ["SELECT MAX(version) as version FROM workflow_executions WHERE id = ?"
                                       execution-id]
                                 {:builder-fn rs/as-unqualified-maps}))
    version))

#_(def debug (atom #{}))

(defn- save-execution* [ds execution]
  #_(locking *out*
    (println "Save " (:execution/id execution) (:execution/version execution) (:execution/comment execution)))
  #_(when (@debug [(:execution/id execution) (:execution/version execution)])
      (throw (ex-info "NO" {:execution execution})))
  #_(swap! debug conj [(:execution/id execution) (:execution/version execution)])
  (with-open [conn (jdbc/get-connection ds)]
    (try
      (record :save-execution* jdbc/execute! conn ["INSERT INTO workflow_executions (
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
                                  (freeze execution)
                                  (:execution/enqueued-at execution)
                                  (:execution/started-at execution)
                                  (:execution/finished-at execution)
                                  (:execution/failed-at execution)
                                  (:execution/step-started-at execution)
                                  (:execution/step-ended-at execution)
                                  (:execution/user-started-at execution)
                                  (:execution/user-ended-at execution)
                                  (freeze (:execution/error execution))
                                  (:execution/comment execution)])
      {:ok    true
       :value execution}
      (catch clojure.lang.ExceptionInfo ei
        (let [expected-unique "23505"]
          (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
            (if (= expected-unique sql-state)
              {:ok    false
               :error (wf/make-error :execution/version-conflict :persistence/execution "failed to save execution"
                                     {:throwable         ei
                                      :execution/id      (:execution/id execution)
                                      :execution/version (:execution/version execution)})}
              (throw ei))
            (throw ei))))
      (catch org.postgresql.util.PSQLException pe
        (let [expected-unique "23505"]
          (if-let [msg (.getServerErrorMessage pe)]
            (if (= expected-unique (.getSQLState msg))
              {:ok    false
               :error (wf/make-error :execution/version-conflict :persistence/execution "failed to save execution"
                                     {:throwable         pe
                                      :execution/id      (:execution/id execution)
                                      :execution/version (:execution/version execution)})}
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
       (try
         (with-open [conn (jdbc/get-connection db-spec)]
           (record :save-statem jdbc/execute! conn ["INSERT INTO workflow_statemachines (id, version, start_at, execution_mode, context, states) VALUES (?, ?, ?, ?, ?, ?)"
                                       (:state-machine/id state-machine)
                                       (:state-machine/version state-machine)
                                       (:state-machine/start-at state-machine)
                                       (:state-machine/execution-mode state-machine)
                                       (freeze (:state-machine/context state-machine))
                                       (freeze (:state-machine/states state-machine))])
           {:ok    true
            :value state-machine})
         (catch clojure.lang.ExceptionInfo ei
           (let [expected-unique "23505"
                 exception
                 (or
                  (when-let [sql-state (:pg-sql-state-error (ex-data ei))]
                    (when (= expected-unique sql-state)
                      (ex-info "state machine already exists with that version"
                               {:error :version-conflict
                                :state-machine/id (:state-machine/id state-machine)
                                :state-machine/version (:state-machine/version state-machine)}
                               ei)))
                  ei)]
             (tap> {::exception exception ::type :save-statem})
             (throw exception)))
         (catch org.postgresql.util.PSQLException pe
           (let [expected-unique "23505"
                 exception
                 (or
                  (when-let [msg (.getServerErrorMessage pe)]
                    (when (= expected-unique (.getSQLState msg))
                      (ex-info "state machine already exists with that version"
                               {:error :version-conflict
                                :state-machine/id (:state-machine/id state-machine)
                                :state-machine/version (:state-machine/version state-machine)}
                               pe)))
                  pe)]
             (tap> {::exception exception ::type :save-statem})
             (throw exception)))))))
  p/ExecutionPersistence
  (executions-for-statem [_ state-machine-id {:keys [version limit offset reverse?]}]
    (with-open [conn (jdbc/get-connection ds)]
      (let [version (resolve-statem-version conn state-machine-id version)]
        (into []
              (db->execution-txfm)
              (jdbc/plan
               conn
               (cond
                 limit [(format "SELECT e.* FROM workflow_executions e
INNER JOIN (
  SELECT id, MAX(version) as version FROM workflow_executions
  WHERE state_machine_id = ? AND state_machine_version = ?
  GROUP BY id
) as e2
ON e.id = e2.id AND e.version = e2.version
WHERE e.state_machine_id = ? AND e.state_machine_version = ?
ORDER BY e.enqueued_at %s LIMIT ? OFFSET ?;"
                                (if reverse? "DESC" "ASC"))
                        state-machine-id version
                        state-machine-id version
                        limit (or offset 0)]
                 :else [(format "SELECT e.* FROM workflow_executions e
INNER JOIN (
  SELECT id, MAX(version) as version FROM workflow_executions
  WHERE state_machine_id = ? AND state_machine_version = ?
  GROUP BY id
) as e2
ON e.id = e2.id AND e.version = e2.version
WHERE e.state_machine_id = ? AND e.state_machine_version = ?
ORDER BY e.enqueued_at %s;"
                                (if reverse? "DESC" "ASC"))
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
  (save-task [_ task]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (let [tid (:task/id task)]
           (try
             (record :save-task jdbc/execute! conn ["INSERT INTO workflow_scheduler_tasks (id, execution_id, start_after, encoded) VALUES (?, ?, ?, ?);"
                                         tid
                                         (:task/execution-id task)
                                         (.getTime ^Date (:task/start-after task))
                                         (freeze task)])
             {:task/id tid}
             (catch clojure.lang.ExceptionInfo ei
               (let [expected-unique "23505"]
                 (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
                   (if (= expected-unique sql-state)
                     {:error (wf/make-error :save-task/version-conflict :persistence/scheduler "failed to save task")}
                     {:error (wf/make-error :save-task/exception :persistence/scheduler "failed to save task" {:error (Throwable->map ei)})})
                   {:error (wf/make-error :save-task/exception :persistence/scheduler "failed to save task" {:error (Throwable->map ei)})})))))))))
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
           (record :complete-task jdbc/execute! conn ["UPDATE workflow_scheduler_tasks SET response = ? WHERE id = ? AND response IS NULL;"
                                       (freeze reply)
                                       task-id])
           {:task/id task-id}
           (catch clojure.lang.ExceptionInfo ei
             (let [expected-unique "23505"]
               (if-let [sql-state (:pg-sql-state-error (ex-data ei))]
                 (if (= expected-unique sql-state)
                   {:error (wf/make-error :complete-task/version-conflict :persistence/scheduler "failed to complete task")}
                   {:error (wf/make-error :complete-task/exception :persistence/scheduler "failed to complete task" {:error (Throwable->map ei)})})
                 {:error (wf/make-error :complete-task/exception :persistence/scheduler "failed to complete task" {:error (Throwable->map ei)})}))))))))
  (delete-task [_ task-id]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (try
           (record :delete-task jdbc/execute! conn ["DELETE FROM workflow_scheduler_tasks WHERE id = ?" task-id])
           (catch clojure.lang.ExceptionInfo ei
             {:error (wf/make-error :delete-task/exception :persistence/scheduler "failed to delete task" {:error (Throwable->map ei)})})))))))

(defn make-scheduler-persistence [db-spec]
  (->SchedulerPersistence db-spec nil nil))

(comment
  (do
    (require '[net.jeffhui.workflow.interpreters :refer [->Sandboxed ->Naive]] :reload)
    (require '[net.jeffhui.workflow.memory :as mem] :reload)
    (require '[net.jeffhui.workflow.contracts :as contracts])
    (defn make [db-spec]
      (let [p (make-persistence db-spec)]
        (wf/effects {:statem      p
                     :execution   p
                     :scheduler   (mem/make-scheduler)
                     :interpreter (->Sandboxed)})))
    (def fx (wf/open (make {:jdbcUrl "jdbc:postgresql://localhost:5432/workflow_dev?user=postgres&password=password"})))
    (wf/save-statem fx contracts/prepare-cart-statem)
    (wf/save-statem fx contracts/order-statem)
    (wf/save-statem fx contracts/shipment-statem)
    (wf/save-statem fx contracts/bad-io-statem)
    (p/register-execution-handler fx (wf/create-execution-handler fx)))

  (wf/close fx)

  (do
    (def out (wf/start fx "prepare-cart" {:skus #{"A1" "B2"}})))

  (do
    (def out (wf/start fx "bad-io" {})))

  (wf/fetch-execution fx (second out) :latest)

  (some (fn [e]
          (some (comp :cause :throwable :error :return ::wf/resume) (:execution/completed-effects e)))
        (wf/fetch-execution-history fx (second out)))

  (some wf/execution-error-truncated
        (wf/fetch-execution-history fx (second out)))

  out

  (do
    (def out (wf/start fx "order" {::wf/io {"http.request.json" (fn [method uri res]
                                                                  {:status 200
                                                                   :body   (:json-body res)})}}))
    (wf/trigger fx (second out) {::wf/action "add"
                                 ::wf/reply? true
                                 :sku        "bns12"
                                 :qty        1})
    #_(Thread/sleep 100)
    (wf/trigger fx (second out) {::wf/action "place"})
    #_(Thread/sleep 100)
    (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                          ::wf/reply? true}))
    (async/take! res prn))


  (defmethod p/io "bad" [_] {:something (delay 1)})
  (wf/io "bad"))
