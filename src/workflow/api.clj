(ns workflow.api
  (:require [workflow.protocol :refer [sleep sleep-to enqueue-execution executor eval-action] :as protocol]
            [workflow.schema :as s]
            ;; for multimethod implementations
            workflow.io.http)
  (:import java.util.UUID
           java.util.concurrent.CompletableFuture
           java.util.function.Supplier
           java.util.concurrent.TimeUnit
           java.util.concurrent.CompletableFuture))

(defn- now! [] (System/nanoTime))

(def ^:dynamic *fx* nil)
(def ^:dynamic *state-machine* nil)
(def ^:dynamic *execution* nil)
(def ^:dynamic *execution-id* nil)

(def ^:private sync-timeout 30000)
(def ^:private async-step-timeout 30000)

(def open protocol/open)
(def close protocol/close)
(def register-execution-handler protocol/register-execution-handler)
(defn fetch-statem [persistence statem-id statem-version]
  (when-let [s (protocol/fetch-statem persistence statem-id statem-version)]
    (s/debug-assert-statem s)))
(defn fetch-execution [persistence execution-id execution-version]
  (when-let [e (protocol/fetch-execution persistence execution-id execution-version)]
    (s/debug-assert-execution e)))
(defn fetch-execution-history [persistence execution-id]
  (map s/debug-assert-execution (protocol/fetch-execution-history persistence execution-id)))
(defn executions-for-statem [persistence state-machine-id options]
  (map s/debug-assert-execution (protocol/executions-for-statem persistence state-machine-id options)))

(defn save-statem [persistence statem]
  (let [errs (s/err-for-statem statem)]
    (if errs
      (future {:ok    false
               :error errs})
      (protocol/save-statem persistence statem))))

;; (def save-execution protocol/save-execution) ;; This isn't common enough to put in this namespace
(defn- save-execution [persistence execution]
  (protocol/save-execution persistence (s/debug-assert-execution execution)))

(defn io-ops [] (keys (methods protocol/io)))

(defn effects [{:keys [statem execution scheduler interpreter]}]
  {:pre [statem execution scheduler interpreter]}
  (protocol/->Effects statem execution scheduler interpreter))

(defn- verify-execution [e]
  (assert (or (not (contains? e :execution/state))
              (:execution/state e)))
  e)

;; QUESTION(jeff): is this better to put in the protocols?
(defn io
  "Processes external io events. Also takes in consideration the any state machine overrides."
  [op & args]
  (let [op (get (:state-machine/io *state-machine*) op op)]
    (if (fn? op)
      (apply op args)
      (apply protocol/io op args))))
(defn- no-io
  [op & args]
  (throw (ex-info "io is not allowed in this location" {:op op :args args})))

(declare run-sync-execution step-execution)
(defn start
  ([fx state-machine-id] (start fx state-machine-id (UUID/randomUUID) nil nil))
  ([fx state-machine-id input] (start fx state-machine-id (UUID/randomUUID) nil input))
  ([fx state-machine-id initial-state input] (start fx state-machine-id (UUID/randomUUID) initial-state input))
  ([fx state-machine-id execution-id initial-state input]
   (if-let [state-machine (s/debug-assert-statem
                           (if (vector? state-machine-id)
                             (fetch-statem fx (first state-machine-id) (second state-machine-id))
                             (fetch-statem fx state-machine-id :latest)))]
     (let [now                   (now!)
           sync?                 (= "sync" (:state-machine/execution-mode state-machine))
           execution             {:execution/comment               (if sync? "Immediately starting execution" "Enqueued for execution")
                                  :execution/event-name            nil
                                  :execution/event-data            nil
                                  :execution/id                    execution-id
                                  :execution/version               1
                                  :execution/state-machine-id      (:state-machine/id state-machine)
                                  :execution/state-machine-version (:state-machine/version state-machine)
                                  :execution/mode                  (:state-machine/execution-mode state-machine)
                                  :execution/status                (if sync? "running" "queued")
                                  :execution/state                 (:state-machine/start-at state-machine)
                                  :execution/memory                (if (nil? initial-state)
                                                                     (eval-action (:state-machine/context state-machine)
                                                                                  fx
                                                                                  no-io
                                                                                  nil
                                                                                  input)
                                                                     initial-state)
                                  :execution/input                 input
                                  :execution/enqueued-at           now
                                  :execution/started-at            (when sync? now)
                                  :execution/finished-at           nil
                                  :execution/failed-at             nil
                                  :execution/step-started-at       (when sync? now)
                                  :execution/step-ended-at         nil
                                  :execution/user-started-at       nil
                                  :execution/user-ended-at         nil
                                  :execution/error                 nil
                                  :execution/return-target         (::return input)
                                  :execution/wait-for              nil
                                  :execution/return-data           nil
                                  :execution/end-state             nil
                                  :execution/dispatch-by-input     nil
                                  :execution/dispatch-result       nil}
           {:keys [begin error]} (executor (:execution/mode (s/debug-assert-execution execution)))]
       (if error
         [false nil {:error error}]
         (begin fx state-machine execution input)))
     [false nil {:error ::state-machine-not-found}])))

(defn trigger [fx execution-id input]
  (if-let [execution (s/debug-assert-execution (fetch-execution fx execution-id :latest))]
    (enqueue-execution fx (:execution/id execution) input)
    {:error ::execution-not-found}))

(defn- timed-run* [timeout-in-msec f]
  (-> (CompletableFuture/supplyAsync (reify Supplier (get [_] (f))))
      (.orTimeout timeout-in-msec TimeUnit/MILLISECONDS)
      (.join)))

(defmacro ^:private timed-run [timeout-in-msec body]
  `(timed-run* ~timeout-in-msec (fn [] ~body)))

(defn- completed-execution? [execution]
  (let [s (:execution/status execution)]
    (or (contains? #{"failed" "waiting"} s)
        (and (= "finished" s)
             (nil? (:execution/state s))))))

(defn- acquire-execution [fx executor-name execution input]
  (let [now (now!)]
    (save-execution fx (merge execution {:execution/version         (inc (:execution/version execution))
                                         :execution/comment         (format "Resuming execution (%s) on %s"
                                                                            (:execution/state execution)
                                                                            executor-name)
                                         :execution/event-data      nil
                                         :execution/event-name      nil
                                         :execution/status          "running"
                                         :execution/started-at      (or (:execution/started-at execution) now)
                                         :execution/error           nil
                                         :execution/failed-at       nil
                                         :execution/user-started-at nil
                                         :execution/user-ended-at   nil
                                         :execution/step-started-at now
                                         :execution/step-ended-at   nil
                                         :execution/finished-at     nil
                                         :execution/input           input}))))

(defn- record-exception [fx starting-execution e]
  (let [err (Throwable->map e)]
    (println "error: " (pr-str starting-execution) "; " (pr-str err))
    (.printStackTrace e)
    (loop [attempt   3
           execution starting-execution]
      (if (zero? attempt)
        (throw (ex-info "Failed to save failure" {:execution starting-execution} e))
        (let [now (now!)]
          (if-let [execution' (save-execution
                               fx
                               (merge execution {:execution/comment       "Error running step"
                                                 :execution/version       (inc (:execution/version execution))
                                                 :execution/error         err
                                                 :execution/status        "failed"
                                                 :execution/step-ended-at now
                                                 :execution/finished-at   now
                                                 :execution/failed-at     now}))]
            execution'
            (do
              (Thread/sleep (* 10 (inc attempt)))
              (recur (dec attempt)
                     (fetch-execution fx (:execution/id execution) (:execution/version execution))))))))))

(defn- can-continue? [state-machine execution input]
  (and (= "paused" (:execution/status execution))
       (let [node (get (:state-machine/states state-machine) (:execution/state execution))]
         (or
          (::action input)
          (seq (:always node))
          (:end node)))))

(defn- maybe-return-execution! [fx execution]
  (when (#{"failed" "finished"} (:execution/status execution))
    (let [[eid action] (:execution/return-target execution)]
      (when (and eid action)
        (trigger fx eid (merge (:execution/return-data execution)
                               {::action action
                                ::cause  {:execution [eid (:execution/status execution)]
                                          :result         (:execution/return-data execution)}}))))))

(defn- run-sync-execution
  "Returns a transducer that manages the database interactions while running
  executions. Avoids using the execution queue by running executions to
  completion.

  Unlike other executions, this is expected to be called synchronously on
  [[start-execution]]. The limitation is the restricted execution time window
  (30s).

  Technically the fastest, since there are no context switches.
  "
  [fx executor-name state-machine]
  (comp
   (remove nil?)
   (map (fn [[starting-execution input]]
          (assert (= (:state-machine/id state-machine) (:execution/state-machine-id starting-execution))
                  (pr-str (:state-machine/id state-machine) (:execution/state-machine-id starting-execution)))
          (let [timeout (or (:state-machine/timeout-msec state-machine) sync-timeout)
                latest-execution (atom starting-execution)]
            (try
              (timed-run
               timeout
               (loop [execution starting-execution]
                 (let [updated-fields (step-execution fx state-machine execution input)
                       now            (now!)
                       entity         (merge execution updated-fields {:execution/step-ended-at now})
                       tx             (save-execution fx entity)]
                   (assert (not= "running" (:execution/status entity)))
                   (reset! latest-execution execution)
                   (if (can-continue? state-machine entity nil)
                     (recur (:entity @(acquire-execution fx executor-name entity nil)))
                     ;; TODO: if we fail to save... is there anything we can do?
                     (if (and (completed-execution? entity) (not (:ok @tx)))
                       (throw (ex-info "Failed to save success in execution"
                                       {:context      entity
                                        :execution-id (:db/id execution)
                                        :timestamp    now}))
                       (do
                         (maybe-return-execution! fx entity)
                         entity))))))
              (catch Exception e
                (record-exception fx @latest-execution e))))))))

(defn- apply-action [start-time execution action]
  (let [action   (merge {:name (:state action)}
                        action)
        wait-for (:wait-for action)
        now      (now!)
        data     (merge (:execution/memory execution) (:context action))]
    #_(s/assert-serialization-size "state-machine state"
                                   (select-keys new-state [:context :state]))
    (cond
      (:state-machine wait-for)
      {:execution/comment         "Paused step by invoking state-machine"
       :execution/event-name      (:name action)
       :execution/event-data      (:metadata action)
       :execution/version         (inc (:execution/version execution))
       :execution/memory          data
       :execution/status          "waiting"
       :execution/wait-for        wait-for
       :execution/user-started-at start-time
       :execution/user-ended-at   now}

      (:seconds wait-for)
      {:execution/comment         (str "Finished step by sleeping for " (:seconds wait-for) "s (" (:execution/state execution) " -> " (:state action) ")")
       :execution/event-name      (:name action)
       :execution/event-data      (:metadata action)
       :execution/version         (inc (:execution/version execution))
       :execution/state           (or (:state action) (:execution/state execution))
       :execution/memory          data
       :execution/status          "waiting"
       :execution/wait-for        wait-for
       :execution/user-started-at start-time
       :execution/user-ended-at   now}

      (:timestamp wait-for)
      {:execution/comment         (str "Finished step by sleeping until timestamp (" (:execution/state execution) " -> " (:state action) ")")
       :execution/event-name      (:name action)
       :execution/event-data      (:metadata action)
       :execution/version         (inc (:execution/version execution))
       :execution/state           (or (:state action) (:execution/state execution))
       :execution/memory          data
       :execution/status          "waiting"
       :execution/wait-for        wait-for
       :execution/user-started-at start-time
       :execution/user-ended-at   now}

      (:state action)
      (merge {:execution/comment         (str "Finished step (" (:execution/state execution) " -> " (:state action) ")")
              :execution/event-name      (:name action)
              :execution/event-data      (:metadata action)
              :execution/version         (inc (:execution/version execution))
              :execution/state           (or (:state action) (:execution/state execution))
              :execution/memory          data
              :execution/status          "paused"
              :execution/wait-for        nil
              :execution/user-started-at start-time
              :execution/user-ended-at   now})

      :else (throw (ex-info (format "state produced unknown action: %s" (pr-str (:state action)))
                            {:action            action
                             :execution-id      (:execution/id execution)
                             :execution-version (:execution/version execution)
                             :state             (:execution/state execution)})))))

(defmacro with-effects [fx state-machine execution & body]
  `(let [e# ~execution]
     (binding [*fx*            ~fx
               *state-machine* ~state-machine
               *execution*     e#
               *execution-id*  (:execution/id e#)]
       ~@body)))

(defn- step-execution
  ([fx state-machine execution] (step-execution fx state-machine execution nil))
  ([fx state-machine execution input]
   (let [estate           (:execution/state execution)
         action-name      (::action input)
         node             (get (:state-machine/states state-machine)
                               estate)
         cause            (::cause input)
         resume           (:execution cause)
         possible-actions (if resume
                            (remove nil? (conj (filter (comp #{action-name} :id) (:always node))
                                               (get (:actions node) action-name)))
                            (not-empty (remove nil? (into (:always node) [(get (:actions node) action-name)]))))
         data             (:execution/memory execution)
         start-time       (now!)]
     (with-effects fx state-machine execution
       (if node
         (if (:end node)
           (let [now (now!)]
             (merge {:execution/comment         (str "Finished execution (" (:execution/state execution) ")"
                                                     (when (:execution/return-target execution)
                                                       " with return value"))
                     :execution/event-name      "exited"
                     :execution/version         (inc (:execution/version execution))
                     :execution/state           nil
                     :execution/end-state       (:execution/state execution)
                     :execution/status          "finished"
                     :execution/return-data     (when (:return node) (eval-action (:return node) fx no-io data input))
                     :execution/wait-for        nil
                     :execution/user-started-at start-time
                     :execution/user-ended-at   now
                     :execution/finished-at     now}))
           (if possible-actions
             ;; TODO(jeff): support node catching exceptions/errors and routing them through a transition or state
             (try
               (loop [possible-actions possible-actions]
                 (if-let [possible-action (first possible-actions)]
                   (if (and (not resume) (:when possible-action) (not (eval-action (:when possible-action) fx no-io data input)))
                     (recur (rest possible-actions))
                     (let [action (cond
                                    (:invoke possible-action)
                                    (let [invoke (:invoke possible-action)]
                                      (cond
                                        (and cause (not= (:wait-for possible-action) (:wait-for cause)))
                                        {:wait-for (:wait-for possible-action)
                                         :state    (:execution/state execution)}

                                        resume
                                        (let [[_ status] resume
                                              choice     (if (= "finished" status)
                                                           (:success invoke)
                                                           (:error invoke))]
                                          (cond-> choice
                                            (:context choice) (update :context eval-action fx no-io data (:execution/dispatch-by-input execution) input)))

                                        (:state-machine invoke)
                                        (merge {:wait-for (merge (select-keys invoke [:state-machine :success :error])
                                                                 (when (:input invoke) {:input (eval-action (:input invoke) fx no-io data input)}))
                                                :state    (:execution/state execution)}
                                               (when (:name invoke) {:name (:name invoke)}))

                                        (:given invoke)
                                        (let [output    (try
                                                          (eval-action (:given invoke) fx io data input)
                                                          (catch Throwable t
                                                            {:error t}))
                                              condition (if (:if invoke)
                                                          (eval-action (:if invoke) fx no-io data input output)
                                                          true)
                                              clause (if condition (:then invoke) (:else invoke))]
                                          (cond-> clause
                                            (:context clause) (update :context eval-action fx no-io data input output)))

                                        :else
                                        (throw (ex-info "Unrecognized invocation type" {:invoke invoke}))))

                                    :else
                                    (cond-> possible-action
                                      (:context possible-action) (update :context eval-action fx no-io data input)))
                           action (update action :name #(or % action-name))

                           new-execution (apply-action start-time execution (merge {:context (:execution/memory execution)} action))]
                       (verify-execution
                        (merge {:execution/error             nil
                                :execution/dispatch-result   nil
                                :execution/dispatch-by-input nil}
                               (when-let [wait-for (:execution/wait-for new-execution)]
                                 (cond
                                   (:state-machine wait-for)
                                   (let [state-machine-id (:state-machine wait-for)
                                         sub-eid          (UUID/randomUUID)
                                         aid              (or (:id possible-action) (:name possible-action))
                                         e-result         (start fx state-machine-id sub-eid
                                                                 (:state wait-for)
                                                                 (merge (:input wait-for)
                                                                        {::return [(:execution/id execution) aid]}))]
                                     (assert aid "Action needs a name or id")
                                     {:execution/dispatch-result   e-result
                                      :execution/dispatch-by-input input})

                                   (:seconds wait-for)
                                   (do (sleep fx (* 1000 (:seconds wait-for)) (:execution/id execution)
                                              (merge {::action  (:state action)
                                                      ::version (inc (:execution/version execution))
                                                      ::cause   {:wait-for wait-for}}
                                                     (:input action)))
                                       nil)

                                   (:timestamp wait-for)
                                   (do (sleep-to fx (:timestamp wait-for)
                                                 (:execution-id execution)
                                                 (merge {::action  (:state action)
                                                         ::version (inc (:execution/version execution))
                                                         ::cause   {:wait-for wait-for}}
                                                        (:input action)))
                                       nil)))
                               new-execution))))
                   {:execution/comment    "No available actions"
                    :execution/event-name "Input Error"
                    :execution/status     "failed-resumable"
                    :execution/version    (inc (:execution/version execution))
                    :execution/error      {:error            "cannot find action in state machine"
                                           :state            estate
                                           :input-action     action-name
                                           :possible-actions possible-actions
                                           :node             node}}))
               (catch Throwable t
                 (let [now (now!)]
                   {:execution/comment         "Failed step with exception"
                    :execution/event-name      "Error"
                    :execution/version         (inc (:execution/version execution))
                    :execution/error           (Throwable->map t)
                    :execution/status          "failed"
                    :execution/wait-for        nil
                    :execution/user-started-at start-time
                    :execution/user-ended-at   now
                    :execution/failed-at       now
                    :execution/finished-at     now})))
             {:execution/comment    "No available actions"
              :execution/event-name "Input Error"
              :execution/status     "failed-resumable"
              :execution/version    (inc (:execution/version execution))
              :execution/error      {:error            "cannot find action in state machine"
                                     :state            estate
                                     :input-action     action-name
                                     :possible-actions (keys (:actions node))}}))
         (let [now (now!)]
           {:execution/comment         "Invalid state"
            :execution/event-name      "Internal Error"
            :execution/version         (inc (:execution/version execution))
            :execution/error           (Throwable->map (ex-info "Unrecognized state"
                                                                {:state           estate
                                                                 :possible-states (set (keys (:state-machine/states state-machine)))}))
            :execution/status          "failed"
            :execution/user-started-at start-time
            :execution/user-ended-at   now
            :execution/finished-at     now
            :execution/failed-at       now}))))))

(defn- run-linear-execution
  "Runs an execution, saving its state into the database executions. Avoids
  using the execution queue by running executions to completion.

  Unlike fair execution, runs each execution to completion before looking at the
  next one. This minimizes context switching costs but can cause a long backlog
  of work if each execution isn't fast to complete.

  Context switches can be very expensive in crux adding 1 poll-wait-duration per
  state transition (assuming we wait for response to ensure write
  acknowledgements).
  "
  [fx state-machine starting-execution input]
  (let [timeout (or (:state-machine/timeout-msec state-machine) async-step-timeout)
        latest-execution (atom starting-execution)]
    (try
      (timed-run
       timeout
       (loop [execution starting-execution
              input     input]
         (let [updated-fields (step-execution fx state-machine execution input)
               now            (now!)
               entity         (merge execution updated-fields {:execution/step-ended-at now})
               tx             (save-execution fx entity)]
           (assert (not= "running" (:execution/status entity))
                   (pr-str updated-fields))
           ; TODO: if we fail to save... is there anything we can do?
           (when (:error entity)
             (throw (ex-info "Failed to save success in execution"
                             {:context      entity
                              :execution-id (:db/id execution)
                              :timestamp    now})))

           (reset! latest-execution execution)
           (if (can-continue? state-machine entity nil)
             (recur (do (deref tx 1000 ::timeout)
                        (:entity @(acquire-execution fx "linear-execution" entity nil)))
                    nil)
             (if (and (completed-execution? entity)
                      (not (:ok @tx)))
               (throw (ex-info "Failed to save success in execution"
                               {:context      entity
                                :execution-id (:db/id execution)
                                :timestamp    now}))
               (do
                 (maybe-return-execution! fx entity)
                 entity))))))
      (catch Exception e
        (record-exception fx latest-execution e)))))

(defn- run-linear-execution-inconsistent
  "Runs an execution, saving its state into the database executions. Avoids
  using the execution queue by running executions to completion.

  Unlike fair execution, runs each execution to completion before looking at the
  next one. This minimizes context switching costs but can cause a long backlog
  of work if each execution isn't fast to complete.

  Context switches can be very expensive in crux adding 1 poll-wait-duration per
  state transition (assuming we wait for response to ensure write
  acknowledgements).
  "
  [fx state-machine starting-execution input]
  (let [timeout (or (:state-machine/timeout-msec state-machine) async-step-timeout)
        latest-execution (atom starting-execution)]
    (try
      (timed-run
       timeout
       (loop [execution starting-execution
              input     input]
         (let [updated-fields (step-execution fx state-machine execution input)
               now            (now!)
               entity         (merge execution updated-fields {:execution/step-ended-at now})
               tx             (save-execution fx entity)]
           (assert (not= "running" (:execution/status entity))
                   (pr-str updated-fields))
           ;; TODO: if we fail to save... is there anything we can do?
           (when (:error entity)
             (throw (ex-info "Failed to save success in execution"
                             {:context      entity
                              :execution-id (:db/id execution)
                              :timestamp    now})))
           (reset! latest-execution execution)
           (if (can-continue? state-machine entity nil)
             (recur (do (acquire-execution fx "linear-execution" entity nil)
                        entity)
                    nil)
             (if (and (completed-execution? entity)
                      (not (:ok @tx)))
               (throw (ex-info "Failed to save success in execution"
                               {:context      entity
                                :execution-id (:db/id execution)
                                :timestamp    now}))
               (do
                 (maybe-return-execution! fx entity)
                 entity))))))
      (catch Exception e
        (record-exception fx latest-execution e)))))

(defn- run-fair-execution
  "Partially runs an execution while managing its corresponding state in the
  database.

  Ensures all its work is executed fairly. That is, executions start as soon as
  possible, rotating between executions to try and ensure some progression. But
  each individual execution runs slower due to increased context switching
  (thus, increased database activity).

  TRADEOFFS:
    This execution model minimized infrastructure impact for any
    slower-than-average execution to dominate resources in each for an
    individual execution running slower.

  WARNING & CAVEATS:
    Context switches can dominate the costs of this execution model. Also
    out-of-order execution is possible with external triggers wanting to advance
    state transitions.
  "
  [fx state-machine execution input]
  (assert (:execution/state-machine-id execution))
  (assert (:execution/state-machine-version execution))
  (try
    (let [updated-fields (step-execution fx state-machine execution input)
          now            (now!)
          tx             @(save-execution fx (merge execution updated-fields {:execution/step-ended-at now}))
          entity         (:entity tx)]
      (assert (not= "running" (:execution/status entity)))
      ;; TODO: if we fail to save... is there anything we can do?
      (when (:error tx)
        (throw (ex-info "Failed to save success in execution"
                        {:context      tx
                         :execution-id (:db/id execution)
                         :timestamp    now})))
      (when (can-continue? state-machine entity nil)
        (enqueue-execution fx (:execution/id entity) nil))
      (maybe-return-execution! fx entity)
      entity)
    (catch Exception e
      (record-exception fx execution e))))

(defn- sync-run
  ([fx state-machine execution] (sync-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (save-execution fx execution)
   (let [final-execution (first (sequence (run-sync-execution fx "synchronous-execution" state-machine)
                                          [execution input]))]
     [(= "finished" (:execution/status final-execution)) (:execution/id execution) final-execution])))
(defn- async-run
  ([fx state-machine execution] (async-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (let [ok (:ok @(save-execution fx execution))]
     (when ok (enqueue-execution fx (:execution/id execution) input))
     [(boolean ok) (when ok (:execution/id execution)) nil])))

(defmethod executor :default [mode] {:error ::unknown-executor :value mode})
(defmethod executor "sync" [_] {:begin sync-run :step sync-run})
(defmethod executor "async-latency-unsafe" [_] {:begin async-run :step run-fair-execution}) ;; WARN(jeff): can have out-of-order execution
(defmethod executor "async-throughput" [_] {:begin async-run :step run-linear-execution})
(defmethod executor "async-throughput-inconsistent" [_] {:begin async-run :step run-linear-execution-inconsistent})

(defn create-execution-handler
  ([fx] (create-execution-handler fx nil))
  ([fx {:keys [executor-name]}]
   (fn handler [eid input]
     (prn "RUN" eid)
     (let [executor-name (or executor-name (.getName (Thread/currentThread)))
           [execution input]
           (loop [attempts 1]
             (when (< attempts 30)
               (when-let [e (fetch-execution fx eid :latest)]
                 (when (:execution/state e)
                   ;;  (prn "ACQUIRE" (:execution/id e) (:execution/version e) (Thread/currentThread))
                   (when-let [res (acquire-execution fx executor-name e input)]
                     (if-let [e (:entity (deref res 10000 nil))]
                       [e input]
                       (do
                         (Thread/sleep (* attempts attempts))
                         (recur (inc attempts)))))))))]
       (when execution
         (let [state-machine  (fetch-statem fx (:execution/state-machine-id execution) (:execution/state-machine-version execution))
               {:keys [step]} (executor (:execution/mode execution))]
           (step fx state-machine execution input)))))))
