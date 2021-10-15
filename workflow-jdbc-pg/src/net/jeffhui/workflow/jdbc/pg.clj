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
           java.util.concurrent.ExecutorService))

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
  (record jdbc/execute! ds ["DROP TABLE IF EXISTS workflow_statemachines"]))

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
	status TEXT,
	state TEXT,
	memory BYTEA,
	input BYTEA,
  io BYTEA,
	enqueued_at BIGINT,
	started_at BIGINT,
	finished_at BIGINT,
	failed_at BIGINT,
	step_started_at BIGINT,
	step_ended_at BIGINT,
	user_started_at BIGINT,
	user_ended_at BIGINT,
	error BYTEA,
	wait_for BYTEA,
	return_target BYTEA,
	return_data BYTEA,
	comment TEXT,
	event_name TEXT,
	event_data BYTEA,
	dispatch_result BYTEA,
	dispatch_by_input BYTEA,
	end_state TEXT,
	PRIMARY KEY (id, version)
  );
  CREATE INDEX IF NOT EXISTS workflow_executions_statem ON workflow_executions (state_machine_id, state_machine_version, started_at);"]))

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
  (let [field->key {:id                    :execution/id
                    :version               :execution/version
                    :state_machine_id      :execution/state-machine-id
                    :state_machine_version :execution/state-machine-version
                    :mode                  :execution/mode
                    :status                :execution/status
                    :state                 :execution/state
                    :memory                :execution/memory
                    :input                 :execution/input
                    :io                    :execution/io
                    :enqueued_at           :execution/enqueued-at
                    :started_at            :execution/started-at
                    :finished_at           :execution/finished-at
                    :failed_at             :execution/failed-at
                    :step_started_at       :execution/step-started-at
                    :step_ended_at         :execution/step-ended-at
                    :user_started_at       :execution/user-started-at
                    :user_ended_at         :execution/user-ended-at
                    :error                 :execution/error
                    :wait_for              :execution/wait-for
                    :return_target         :execution/return-target
                    :return_data           :execution/return-data
                    :comment               :execution/comment
                    :event_name            :execution/event-name
                    :event_data            :execution/event-data
                    :dispatch_result       :execution/dispatch-result
                    :dispatch_by_input     :execution/dispatch-by-input
                    :end_state             :execution/end-state}]
    (comp
     (map #(set/rename-keys % field->key))
     (map #(select-keys % (vals field->key)))
     (map #(update % :execution/memory nippy/fast-thaw))
     (map #(update % :execution/input nippy/fast-thaw))
     (map #(update % :execution/io nippy/fast-thaw))
     (map #(update % :execution/error nippy/fast-thaw))
     (map #(update % :execution/wait-for nippy/fast-thaw))
     (map #(update % :execution/return-target nippy/fast-thaw))
     (map #(update % :execution/return-data nippy/fast-thaw))
     (map #(update % :execution/event-data nippy/fast-thaw))
     (map #(update % :execution/dispatch-result nippy/fast-thaw))
     (map #(update % :execution/dispatch-by-input nippy/fast-thaw)))))

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

(defn- save-execution [ds execution]
  (with-open [conn (jdbc/get-connection ds)]
    (try
      (record jdbc/execute! conn ["INSERT INTO workflow_executions (
      id, version, state_machine_id, state_machine_version, mode, status, state, memory, input, io,
      enqueued_at, started_at, finished_at, failed_at, step_started_at, step_ended_at,
      user_started_at, user_ended_at, error, wait_for, return_target, return_data, comment, event_name, event_data,
      dispatch_result, dispatch_by_input, end_state)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                                  (:execution/id execution)
                                  (:execution/version execution)
                                  (:execution/state-machine-id execution)
                                  (:execution/state-machine-version execution)
                                  (:execution/mode execution)
                                  (:execution/status execution)
                                  (:execution/state execution)
                                  (nippy/fast-freeze (:execution/memory execution))
                                  (nippy/fast-freeze (:execution/input execution))
                                  (nippy/fast-freeze (:execution/io execution))
                                  (:execution/enqueued-at execution)
                                  (:execution/started-at execution)
                                  (:execution/finished-at execution)
                                  (:execution/failed-at execution)
                                  (:execution/step-started-at execution)
                                  (:execution/step-ended-at execution)
                                  (:execution/user-started-at execution)
                                  (:execution/user-ended-at execution)
                                  (nippy/fast-freeze (:execution/error execution))
                                  (nippy/fast-freeze (:execution/wait-for execution))
                                  (nippy/fast-freeze (:execution/return-target execution))
                                  (nippy/fast-freeze (:execution/return-data execution))
                                  (:execution/comment execution)
                                  (:execution/event-name execution)
                                  (nippy/fast-freeze (:execution/event-data execution))
                                  (nippy/fast-freeze (:execution/dispatch-result execution))
                                  (nippy/fast-freeze (:execution/dispatch-by-input execution))
                                  (:execution/end-state execution)])
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
  (save-statem [_ state-machine]
    (.submit
     pool
     ^Callable
     (fn []
       (with-open [conn (jdbc/get-connection db-spec)]
         (record jdbc/execute! conn ["INSERT INTO workflow_statemachines (id, version, start_at, execution_mode, context, states) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING"
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
ORDER BY e.started_at DESC LIMIT ? OFFSET ?;"
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
ORDER BY e.started_at DESC;"
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
  (save-execution [_ execution]
    (.submit pool ^Callable (fn [] (save-execution ds execution)))))

(defn make-persistence [db-spec]
  (->Persistence db-spec nil nil))

(comment

  (def store (make-persistence {:jdbcUrl "jdbc:postgresql://localhost:5432/workflow_dev?user=postgres&password=password"}))
  (def store (p/open store))
  (p/close store)

  (time (p/fetch-statem store "order" 1))

  (def res
    (p/save-statem store #:state-machine{:id             "order"
                                         :version        1
                                         :start-at       "create"
                                         :execution-mode "async-throughput"
                                         :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                                         :states         '{"create"         {:always [{:name  "created"
                                                                                       :state "cart"}]}
                                                           "cart"           {:actions {"add"    {:name    "added"
                                                                                                 :state   "cart"
                                                                                                 :context (update-in state [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
                                                                                       "remove" {:name    "removed"
                                                                                                 :state   "cart"
                                                                                                 :context (letfn [(sub [a b]
                                                                                                                    (let [a (vec a)
                                                                                                                          n (count a)]
                                                                                                                      (loop [out (transient [])
                                                                                                                             i   0
                                                                                                                             b   (frequencies b)]
                                                                                                                        (if (= i n)
                                                                                                                          (persistent! out)
                                                                                                                          (let [ai (a i)]
                                                                                                                            (if (pos? (b ai 0))
                                                                                                                              (recur out (inc i) (update b ai dec))
                                                                                                                              (recur (conj! out ai) (inc i) b)))))))]
                                                                                                            (update-in state [:order :line-items] (fnil sub []) (repeat (:qty input 1) (:sku input))))}
                                                                                       "place"  {:state "submitted"}}}
                                                           "submitted"      {:actions {"fraud-approve" {:state "fraud-approved"}
                                                                                       "fraud-reject"  {:state "fraud-rejected"}}}

                                                           "fraud-approved" {:always [{:state "released"}]}
                                                           "fraud-rejected" {:actions {"cancel" {:state "canceled"}}}
                                                           "released"       {:always [{:id     "ship"
                                                                                       :name   "ship"
                                                                                       :invoke {:state-machine ["shipment" 1]
                                                                                                :input         {:order (:id (:order state))}
                                                                                                :success       {:state   "ship-finished"
                                                                                                                :context {:delivered (:delivered output)}}
                                                                                                :error         {:state "canceled"}}}]}
                                                           "ship-finished"  {:always [{:name  "fulfilled"
                                                                                       :when  (:delivered state)
                                                                                       :state "shipped"}
                                                                                      {:name  "canceled"
                                                                                       :state "canceled"}]}
                                                           "shipped"        {:end true}
                                                           "canceled"       {:end true}}}))

  (p/close fx)
  (do
    (require '[net.jeffhui.workflow.interpreters :refer [->Sandboxed ->Naive]]
             '[net.jeffhui.workflow.memory :as mem]
             '[clojure.core.async :as async]
             :reload)
    (defn make
      ([db-spec] (let [store (make-persistence db-spec)]
                   (wf/effects {:statem      store
                                :execution   store
                                :scheduler   (mem/make-scheduler)
                                :interpreter (->Sandboxed)})))
      ([db-spec buf-size] (let [store (make-persistence db-spec)]
                            (wf/effects {:statem      store
                                         :execution   store
                                         :scheduler   (mem/make-scheduler buf-size)
                                         :interpreter (->Sandboxed)}))))
    (def fx (p/open (make {:jdbcUrl "jdbc:postgresql://localhost:5432/workflow?user=postgres&password=password"})))
    (wf/save-statem fx #:state-machine{:id             "order"
                                       :version        1
                                       :start-at       "create"
                                       :execution-mode "async-throughput"
                                       :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                                       :states         '{"create"         {:always [{:name  "created"
                                                                                     :state "cart"}]}
                                                         "cart"           {:actions {"add"    {:name    "added"
                                                                                               :state   "cart"
                                                                                               :context (update-in state [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
                                                                                     "remove" {:name    "removed"
                                                                                               :state   "cart"
                                                                                               :context (letfn [(sub [a b]
                                                                                                                  (let [a (vec a)
                                                                                                                        n (count a)]
                                                                                                                    (loop [out (transient [])
                                                                                                                           i   0
                                                                                                                           b   (frequencies b)]
                                                                                                                      (if (= i n)
                                                                                                                        (persistent! out)
                                                                                                                        (let [ai (a i)]
                                                                                                                          (if (pos? (b ai 0))
                                                                                                                            (recur out (inc i) (update b ai dec))
                                                                                                                            (recur (conj! out ai) (inc i) b)))))))]
                                                                                                          (update-in state [:order :line-items] (fnil sub []) (repeat (:qty input 1) (:sku input))))}
                                                                                     "place"  {:state "submitted"}}}
                                                         "submitted"      {:actions {"fraud-approve" {:state "fraud-approved"}
                                                                                     "fraud-reject"  {:state "fraud-rejected"}}}

                                                         "fraud-approved" {:always [{:state "released"}]}
                                                         "fraud-rejected" {:actions {"cancel" {:state "canceled"}}}
                                                         "released"       {:always [{:id     "ship"
                                                                                     :name   "ship"
                                                                                     :invoke {:state-machine ["shipment" 1]
                                                                                              :input         {:order (:id (:order state))}
                                                                                              :success       {:state   "ship-finished"
                                                                                                              :context {:delivered (:delivered output)}}
                                                                                              :error         {:state "canceled"}}}]}
                                                         "ship-finished"  {:always [{:name  "fulfilled"
                                                                                     :when  (:delivered state)
                                                                                     :state "shipped"}
                                                                                    {:name  "canceled"
                                                                                     :state "canceled"}]}
                                                         "shipped"        {:end true}
                                                         "canceled"       {:end true}}})
    (wf/save-statem fx #:state-machine{:id             "shipment"
                                       :version        1
                                       :start-at       "created"
                                       :execution-mode "async-throughput"
                                       :context        '{:id        "S1"
                                                         :order     (:order input)
                                                         :delivered false}
                                       :states         '{"created"     {:always [{:name  "fulfilled"
                                                                                  :state "outstanding"}]}
                                                         "outstanding" {:always  [{:name   "fetched"
                                                                                   :invoke {:given #_{:status 200
                                                                                                      :body   {:json {:n (rand-int 10)}}}
                                                                                            (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}}) :if
                                                                                            (<= 200 (:status output) 299) :then
                                                                                            {:state   "fetched"
                                                                                             :context {:response {:n (:n (:json (:body output)))}}} :else
                                                                                            {:state "failed"}}}]
                                                                        :actions {"cancel" {:state "canceled"}}}
                                                         "failed"      {:always [{:name     "retry"
                                                                                  :state    "outstanding"
                                                                                  :wait-for {:seconds 5}}]}

                                                         "canceled"    {:end    true
                                                                        :return {:delivered false}}

                                                         "fetched"     {:always [{:name    "deliver"
                                                                                  :state   "delivered"
                                                                                  :when    (> 3 (:n (:response state)))
                                                                                  :context {:response nil
                                                                                            :result   (:n (:response state))}}
                                                                                 {:name     "retry"
                                                                                  :state    "outstanding"
                                                                                  :context  {:response nil}
                                                                                  :wait-for {:seconds 5}}]}

                                                         "delivered"   {:end    true
                                                                        :return {:delivered true}}}})
    (p/register-execution-handler fx (wf/create-execution-handler fx))
    (def out (wf/start fx "order" nil))
    (do
      (wf/trigger fx (second out) {::wf/action "add"
                                   ::wf/reply? true
                                   :sku       "bns12"
                                   :qty       1})
      #_(Thread/sleep 100)
      (wf/trigger fx (second out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (println "START" (System/nanoTime))
      (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                            ::wf/reply? true}))
      (async/take! res (fn [x] (println "END" (System/nanoTime) (pr-str x)))))
    )

  (double (/ (- 593025224240861 593025080765764)
             1000000))

  (clojure.pprint/print-table
   (sort-by
    (juxt :t)
    (map (fn [sm]
           (merge {:execution/event-name ""}
                  (select-keys (assoc sm :t [(or (:execution/step-started-at sm) (:execution/enqueued-at sm))
                                             (:execution/version sm)])
                               [:execution/state-machine-id
                                :execution/state
                                :execution/status
                                :execution/event-name
                                :execution/comment
                                :execution/input
                                :t
                                :execution/error
                                :execution/memory])
                  {:duration-ms      (when (and (:execution/step-started-at sm) (:execution/step-ended-at sm))
                                       (double
                                        (/
                                         (- (:execution/step-ended-at sm)
                                            (:execution/step-started-at sm))
                                         1000000)))
                   :user-duration-ms (when (and (:execution/user-started-at sm) (:execution/user-ended-at sm))
                                       (double
                                        (/
                                         (- (:execution/user-end sm)
                                            (:execution/user-start sm))
                                         1000000)))}))
         (into
          (wf/executions-for-statem fx "order" {:version :latest})
          (wf/executions-for-statem fx "shipment" {:version :latest})))))

  (with-open [conn (:ds (:execution-persistence fx))]
    (jdbc/execute! conn ["SELECT * FROM workflow_executions"]))
  )
