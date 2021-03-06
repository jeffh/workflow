(ns net.jeffhui.workflow.api
  (:require [net.jeffhui.workflow.protocol :refer [sleep sleep-to enqueue-execution executor eval-action] :as protocol]
            [net.jeffhui.workflow.core :as core]
            [net.jeffhui.workflow.schema :as s]
            ;; for multimethod implementations
            net.jeffhui.workflow.io.http
            [clojure.core.async :as async]
            [clojure.set :as set]
            [clojure.walk :as walk])
  (:import java.util.UUID
           java.util.function.Supplier
           java.util.concurrent.TimeUnit
           java.util.concurrent.CompletableFuture))

(defn- now! [] (System/nanoTime))

(def ^:dynamic *fx*
  "The effect instance, available during io method implementations"
  nil)
(def ^:dynamic *state-machine*
  "The current state-machine, available during io method implementations"
  nil)
(def ^:dynamic *execution*
  "The current execution, available during io method implementations"
  nil)
(def ^:dynamic *execution-id*
  "The current execution-id, available during io method implementations"
  nil)

(defn io-ops
  "Returns all available io operations registered."
  [] (keys (methods protocol/io)))

(def ^:private sync-timeout 30000)
(def ^:private async-step-timeout 30000)

(defn open
  "Opens stateful connections for effects.

  Passthrough to [[protocol/open]]"
  [obj]
  (protocol/open obj))
(defn close
  "Closes stateful connections for effects.

  Passthrough to [[protocol/close]]"
  [obj]
  (protocol/close obj))

(declare create-execution-handler)
(defn register-execution-handler
  "Opts into processing executions.

  Note: before calling this, all [[io]] methods should be defined.

  Attaches a function responsible to run executions.

  Passing a nil handler stops processing executions.
  Omitting a handler uses [[create-execution-handler]] by default.
  "
  ([fx] (register-execution-handler fx (create-execution-handler fx)))
  ([fx handler] (protocol/register-execution-handler fx handler)))

(defn fetch-statem
  "Returns a specific version of a state machine.

  If version is `:latest`, then the persistence layer will return the state machine with the highest version.
  "
  [fx statem-id statem-version]
  (when-let [s (protocol/fetch-statem fx statem-id statem-version)]
    (s/debug-assert-statem s)))


(defn fetch-execution
  "Returns a specific version of an execution.

  A version of an execution represents a specific moment in time of an execution.

  If version is `:latest`, then the persistence layer will return the execution with the highest version.
  "
  [fx execution-id execution-version]
  (when-let [e (protocol/fetch-execution fx execution-id execution-version)]
    (s/debug-assert-execution e)))

(defn fetch-execution-history
  "Returns a sequence of the full history of an execution, ordered by execution history (starting the execution is first)."
  [fx execution-id]
  (map s/debug-assert-execution (protocol/fetch-execution-history fx execution-id)))

(defn executions-for-statem
  "Returns a sequence of executions by a given state machine. Ordered by latest executions enqueued-at.

    Parameters:
      options - {:limit int, :offset int, :version #{:latest, int, :all}}
        NOTE for implementations of this protocol: version is resolved by the [[Effects]] type to an integer.
        If version is :latest, returns the latest version of each execution.
        If version is :all, returns the all versions of execution, including historical executions.
    "
  [fx state-machine-id options]
  (map s/debug-assert-execution (protocol/executions-for-statem fx state-machine-id options)))

(defn execution-error
  "Returns an error stored in the execution"
  [execution]
  (or (when-let [e (:execution/error execution)]
        e)
      (when-let [ce (:execution/completed-effects execution)]
        (when-let [e (some (comp :error :return ::resume) ce)]
          e))))

(defn execution-error-truncated
  "Returns a truncated version of the error stored in the execution"
  [execution]
  (let [trace-> (fn [t]
                  {:message (format "%s: %s" (:type t) (:message t))
                   :data    (:data t)
                   :at      (:at t)})]
    (or (when-let [e (:execution/error execution)]
          {:execution/id      (:execution/id execution)
           :execution/version (:execution/version execution)
           :kind              :execution-exception
           :message           (:cause e)
           :recent-stack      (vec (take 6 (map trace-> (:via e))))})
        (when-let [ce (:execution/completed-effects execution)]
          (when-let [e (some (comp :error :return ::resume) ce)]
            {:execution/id      (:execution/id execution)
             :execution/version (:execution/version execution)
             :kind              :effect-error
             :message           (:cause (:throwable e))
             :code              (:code e)
             :recent-stack      (vec (take 6 (map trace-> (:via (:throwable e)))))})))))

(defn save-statem
  "Saves a specific version a state machine.

   Parameters:
    state-machine - the state machine to save
    options - currently unused. Exists for future use.

   Returns a (future {:ok bool, :entity {saved-state-machine...}})"
  [persistence statem]
  (let [errs (s/err-for-statem statem)]
    (if errs
      (future {:ok    false
               :error errs})
      (protocol/save-statem persistence statem nil))))

;; (def save-execution protocol/save-execution) ;; This isn't common enough to put in this namespace
(defn- save-execution [persistence execution options]
  (protocol/save-execution persistence (s/debug-assert-execution execution) options))

(defn effects [{:keys [statem execution scheduler interpreter]}]
  {:pre [statem execution scheduler interpreter]}
  (protocol/->Effects statem execution scheduler interpreter))

(defn- verify-execution [e]
  (assert (or (not (contains? e :execution/state))
              (:execution/state e)))
  e)

;; QUESTION(jeff): is this better to put in the protocols?
(defn io
  "Processes external io events. Also takes in consideration the any execution overrides."
  [op & args]
  (-> (if-let [code (get (:execution/io *execution*) op)]
        (let [res (eval-action code *fx* io (:execution/memory *execution*) nil)]
          (if (fn? res)
            (apply res args)
            res))
        (apply protocol/io op args))
      (s/assert-edn (format "(io %s ...) must return edn, but didn't" (pr-str op)))))

(defn- no-io
  [op & args]
  (throw (ex-info "io is not allowed in this location" {:op op :args args})))

(defn empty-execution
  "Returns a map that represents an empty execution"
  [{:keys [execution-id state-machine initial-context input now return-to] :as options}]
  (let [now   (or now (now!))
        mode  (:state-machine/execution-mode state-machine)
        sync? (= mode "sync")]
    (merge
     (core/empty-execution (assoc options :return-to (or return-to (::return-to input) (::core/return-to input))))
     {:execution/comment           (if sync? "Immediately starting execution" "Enqueued for execution")
      :execution/mode              mode
      :execution/enqueued-at       now
      :execution/started-at        (when sync? now)
      :execution/finished-at       nil
      :execution/failed-at         nil
      :execution/step-started-at   (when sync? now)
      :execution/step-ended-at     nil
      :execution/user-started-at   nil
      :execution/user-ended-at     nil
      :execution/pending-effects   nil
      :execution/completed-effects nil
      :execution/error             nil})))

(declare run-sync-execution step-execution!)
(defn start
  "Starts an execution.

  state-machine-id = string? | [string? int?]
  execution-id = uuid?

  Note: It isn't safe to for input to come from untrusted sources.
   - Recommended: validate input values before passing through this function.
   - Less Good Alternative: pass the untrusted input as a key in input.
  "
  ([fx state-machine-id]       (start fx state-machine-id (UUID/randomUUID) nil))
  ([fx state-machine-id input] (start fx state-machine-id (UUID/randomUUID) input))
  ([fx state-machine-id execution-id input]
   (if-let [state-machine (if (vector? state-machine-id)
                            (fetch-statem fx (first state-machine-id) (second state-machine-id))
                            (fetch-statem fx state-machine-id :latest))]
     (do
       (s/debug-assert-statem state-machine {:requested-state-machine-id state-machine-id})
       (let [now                   (now!)
             execution             (empty-execution {:execution-id    execution-id
                                                     :state-machine   state-machine
                                                     :initial-context (eval-action (:state-machine/context state-machine)
                                                                                   fx
                                                                                   no-io
                                                                                   nil
                                                                                   input)
                                                     :input           input
                                                     :now             now})
             input                 (:execution/input input)
             {:keys [begin error]} (executor (:execution/mode (s/debug-assert-execution execution)))]
         (cond
           error        [false nil {:error error}]
           (nil? begin) [false nil {:error "invalid executor"}]
           :else        (begin fx state-machine execution input))))
     [false nil {:error               ::state-machine-not-found
                 :requested-statem-id state-machine-id}])))

(defn trigger
  "Triggers a state machine to follow a specific transition.

  Note: It isn't safe to for input to come from untrusted sources.
   - Recommended: validate input values before passing through this function.
   - Less Good Alternative: pass the untrusted input as a key in input.
  "
  [fx execution-id input]
  (when-not (::action input)
    (throw (IllegalArgumentException. (format "::action is missing from input: %s" (pr-str input)))))
  (if-let [execution (s/debug-assert-execution (fetch-execution fx execution-id :latest))]
    (enqueue-execution fx (:execution/id execution) input)
    (throw (ex-info "execution not found" {:execution/id execution-id}))))

(defn- timed-run* [timeout-in-msec f]
  (try
    (-> (CompletableFuture/supplyAsync (reify Supplier (get [_] (f))))
        (.orTimeout timeout-in-msec TimeUnit/MILLISECONDS)
        (.join))
    (catch java.util.concurrent.CompletionException e
      (throw (.getCause e)))))

(defmacro ^:private timed-run [timeout-in-msec body]
  `(let [t# ~timeout-in-msec]
     (if (and t# (pos? t#))
       (timed-run* t# (fn [] ~body))
       ~body)))

(defn- completed-execution? [execution]
  (let [s (:execution/pause-state execution)]
    (or (contains? #{"failed" "waiting"} s)
        (and (= "finished" s)
             (nil? (:execution/state s))))))

(defn- acquire-execution [fx executor-name execution input options]
  (let [now (now!)]
    (save-execution fx (merge execution {:execution/version         (inc (:execution/version execution))
                                         :execution/comment         (format "Resuming execution (%s) on %s"
                                                                            (:execution/state execution)
                                                                            executor-name)
                                         :execution/started-at      (or (:execution/started-at execution) now)
                                         :execution/error           nil
                                         :execution/failed-at       nil
                                         :execution/user-started-at nil
                                         :execution/user-ended-at   nil
                                         :execution/step-started-at now
                                         :execution/step-ended-at   nil
                                         :execution/finished-at     nil
                                         :execution/input           input})
                    options)))

(defn- record-exception [fx starting-execution e]
  (let [err          (Throwable->map e)
        max-attempts 3]
    (loop [attempt   max-attempts
           execution starting-execution]
      (if (zero? attempt)
        (throw (ex-info "Failed to save failure" {:execution/id      (:execution/id starting-execution)
                                                  :execution/version (:execution/version starting-execution)
                                                  :error             :out-of-retry-attempts
                                                  :attempts          max-attempts}
                        e))
        (let [now (now!)]
          (if-let [execution' (save-execution
                               fx
                               (merge execution {:execution/comment       "Error running step"
                                                 :execution/version       (inc (:execution/version execution))
                                                 :execution/error         err
                                                 :execution/step-ended-at now
                                                 :execution/failed-at     now})
                               {:can-fail? false})]
            execution'
            (do
              (Thread/sleep (* 10 (inc attempt)))
              (recur (dec attempt)
                     (fetch-execution fx (:execution/id execution) (:execution/version execution))))))))))

(defn- random-uuid [] (UUID/randomUUID))
(defn- make-cofx [fx io]
  {:eval-expr!           (fn eval-expr!
                           ([expr context input] (eval-action expr fx no-io context input))
                           ([expr context input output] (eval-action expr fx no-io context input output)))
   :current-time!        now!
   :random-resume-id!    random-uuid
   :random-execution-id! random-uuid})

(defn- run-sync-execution
  "Returns the final transaction. Also manages the database interactions while
  running executions. Avoids using the execution queue by running executions to
  completion.

  Unlike other executions, this is expected to be called synchronously on
  [[start-execution]]. The limitation is the restricted execution time window
  (30s is default).

  Technically the fastest, since there are no context switches.
  "
  [fx executor-name state-machine starting-execution input]
  (assert (= (:state-machine/id state-machine) (:execution/state-machine-id starting-execution))
          (pr-str (:state-machine/id state-machine) (:execution/state-machine-id starting-execution)))
  (let [timeout          (or (:state-machine/timeout-msec state-machine) sync-timeout)
        latest-execution (atom starting-execution)
        cofx             (make-cofx fx io)]
    (try
      (timed-run
       timeout
       (loop [execution starting-execution
              input     input]
         (let [[stop? next-execution] (step-execution! cofx fx state-machine execution input)
               _                      (reset! latest-execution execution)
               ;; TODO(jeff): save intermediate executions (under (map second transitions))
               tx                     (save-execution fx next-execution {:can-fail? (not stop?)})]
           (if stop?
             (if (:ok @tx)
               next-execution
               (throw (ex-info "Failed to save execution"
                               {:execution/id      (:execution/id execution)
                                :execution/version (:execution/version execution)
                                :tx                @tx})))
             (recur (:entity @(acquire-execution fx executor-name next-execution nil {:can-fail? true}))
                    nil)))))
      (catch Exception e
        (record-exception fx @latest-execution e)))))

(defmacro with-effects [fx state-machine execution & body]
  `(let [e# ~execution]
     (binding [*fx*            ~fx
               *state-machine* ~state-machine
               *execution*     e#
               *execution-id*  (:execution/id e#)]
       ~@body)))

(defn- should-stop? [execution effects error]
  (or (boolean error)
      (and (contains? #{"wait-fx" "await-input" "finished"} (:execution/pause-state execution))
           (empty? effects)
           (empty? (:execution/completed-effects execution)))))

(defn- throwable->map [t]
  (let [m (Throwable->map t)

        scrub-data (fn scrub-data [v]
                     (walk/postwalk
                      (fn [f]
                        (if (or (string? f)
                                (number? f)
                                (map? f)
                                (seq? f)
                                (list? f)
                                (set? f)
                                (vector? f)
                                (keyword? f)
                                (nil? f)
                                (record? f)
                                (symbol? f)
                                (inst? f)
                                (uuid? f))
                          f
                          #:net.jeffhui.workflow.api.serialization-error{:type        (.getName (type f))
                                                                         :stringified (pr-str f)}))
                      v))
        scrub-err  (fn scrub-err [e]
                    (if (:data e)
                      (update e :data scrub-data)
                      e))]
    (update m :via
            (fn [errs]
              (mapv scrub-err errs)))))

(defn- step-execution! [cofx fx state-machine execution input]
  ;; Advances the execution, running side effects if needed
  ;;
  ;; Pipeline: Trigger/Step -(1-to-many)-> Pending Effects -(1-to-many)-> Completed Effects
  ;;
  ;; Algorithm:
  ;;  1. Run any incoming input trigger
  ;;  2. Execution any pending effects
  ;;     TODO(jeff): we should process pending effects one at a time
  ;;  3. Process any completion effects
  ;;  4. Automate decision making
  (cond
    ;; we're processing an action that has input from a trigger
    input
    (let [{:keys [effects transitions error] :as result} (core/step-execution cofx state-machine (assoc execution :execution/error nil)
                                                                              (set/rename-keys input {::action ::core/action
                                                                                                      ::resume ::core/resume}))
          now                                            (now!)
          next-execution                                 (-> (:execution result)
                                                             (assoc :execution/error error
                                                                    :execution/step-ended-at now
                                                                    :execution/comment "Processed (input) step")
                                                             (update :execution/pending-effects (comp not-empty (fnil into [])) effects))
          stop?                                          (should-stop? next-execution effects error)]
      [stop? (cond-> next-execution
               stop? (assoc :execution/finished-at now))])

    ;; we have effects to run
    ;; TODO(jeff): do we want to batch this, or is it better to do them one-at-a-time and go to persist?
    (:execution/pending-effects execution)
    (let [next-execution
          (with-effects fx state-machine execution
            (reduce (fn [execution {:keys [op args complete-ref] :as ef}]
                      (let [ret (try
                                  ;; TODO(jeff): ops should validate args
                                  ;; TODO(jeff): move this case into protocol namespace
                                  (condp = op
                                    :execution/start (let [{:keys [state-machine-id execution-id input async?]}
                                                           args

                                                           [ok execution-id execution]
                                                           (start fx state-machine-id execution-id input)]
                                                       (when async?
                                                         (merge (when execution {:execution execution})
                                                                {:error        (if ok nil {:code   :error
                                                                                           :reason "failed to start"})
                                                                 :execution/id execution-id})))
                                    :execution/step (let [{:keys [execution-id action input async?]}
                                                          args

                                                          result
                                                          (trigger fx execution-id (merge input
                                                                                          {::action input}
                                                                                          (when async?
                                                                                            {::reply? true})))]
                                                      (when result
                                                        (async/alt!!
                                                          (async/timeout 10000) {:error {:code   :timed-out
                                                                                         :reason "waiting for result to trigger execution has timed out"}}
                                                          result ([x] x))))
                                    :execution/return (let [to           (:to args)
                                                            return-value (:result args)]
                                                        (try
                                                          (doseq [[execution-id complete-id] to]
                                                            (enqueue-execution fx execution-id {::resume {:id     complete-id
                                                                                                          :return return-value}
                                                                                                ::reply? false}))
                                                          (catch Throwable t
                                                            (.printStackTrace t))))
                                    :invoke/io        (let [{:keys [input expr]} args]
                                                        (try
                                                          (let [result (protocol/eval-action expr fx io (:execution/memory execution) input)]
                                                            {:value result})
                                                          (catch Throwable t
                                                            {:error {:code      :error
                                                                     :input     input
                                                                     :expr      expr
                                                                     :throwable (throwable->map t)}})))
                                    :sleep/seconds    (let [{:keys [seconds]} args
                                                            ok                (protocol/sleep fx seconds (:execution/id execution)
                                                                                              {::resume {:id     complete-ref
                                                                                                         :return {:sleep seconds}}})]
                                                        (when-not ok
                                                          {:error {:code   :io
                                                                   :reason "failed to schedule sleep by seconds"}}))
                                    :sleep/timestamp  (let [{:keys [timestamp]} args
                                                            ok                  (protocol/sleep-to fx timestamp (:execution/id execution)
                                                                                                   {::resume {:id     complete-ref
                                                                                                              :return {:timestamp timestamp}}})]
                                                        (when-not ok
                                                          {:error {:code   :io
                                                                   :reason "failed to schedule sleep until timestamp"}})))
                                  (catch Throwable t
                                    {:error {:code      :error
                                             :throwable (throwable->map t)}}))]
                        (cond-> (-> execution
                                    (assoc :execution/step-ended-at (now!)
                                           :execution/comment (format "Ran effects"))
                                    (update :execution/pending-effects next)
                                    (update :execution/version inc))
                          (and ret complete-ref) (update :execution/completed-effects (fnil conj [])
                                                         {::resume {:id     complete-ref
                                                                    :op     op
                                                                    :return (assoc ret :ok (let [err (:error ret)]
                                                                                             (boolean (not err))))}}))))
                    execution
                    (:execution/pending-effects execution)))
          stop? (should-stop? next-execution (:execution/pending-effects next-execution) (:execution/error execution))]
      [stop? (cond-> next-execution
               stop? (assoc :execution/finished-at (now!)))])

    ;; advance the state machine based on effects that have completed that the
    ;; execution wants to be notified about success/failure
    (:execution/completed-effects execution)
    (let [effect-result                                  (first (:execution/completed-effects execution))
          {:keys [effects transitions error] :as result} (core/step-execution cofx state-machine (assoc execution :execution/error nil)
                                                                              (set/rename-keys effect-result {::action ::core/action
                                                                                                              ::resume ::core/resume}))
          now                                            (now!)
          next-execution                                 (-> (:execution result)
                                                             (assoc :execution/error error
                                                                    :execution/pending-effects effects
                                                                    :execution/step-ended-at now
                                                                    :execution/comment "Processed effect result")
                                                             (update :execution/completed-effects (comp not-empty subvec) 1))
          stop?                                          (should-stop? next-execution effects error)]
      [stop? (cond-> next-execution
               stop? (assoc :execution/finished-at now))])

    ;; advance possibly an automated state machine advance
    :else
    (let [{:keys [effects transitions error] :as result} (core/step-execution cofx state-machine (assoc execution :execution/error nil)
                                                                              (set/rename-keys input {::action ::core/action
                                                                                                      ::resume ::core/resume}))
          now                                            (now!)
          next-execution                                 (assoc (:execution result)
                                                                :execution/error error
                                                                :execution/pending-effects effects
                                                                :execution/step-ended-at now
                                                                :execution/comment "Processed step")
          stop?                                          (should-stop? next-execution effects error)]
      [stop? (cond-> next-execution
               stop? (assoc :execution/finished-at now))])))

(defn- run-linear-execution
  "Runs an execution, saving its state into the database executions. Avoids
  using the execution queue by running executions to completion.

  Unlike fair execution, runs each execution to completion before looking at the
  next one. This minimizes context switching costs but can cause a long backlog
  of work if each execution isn't fast to complete.

  Context switches can be very expensive in polling-based infrastructure state
  transition (assuming we wait for response to ensure write acknowledgements).
  "
  [fx state-machine starting-execution input]
  (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
        latest-execution (atom starting-execution)
        cofx             (make-cofx fx io)]
    (try
      (timed-run
       timeout
       (loop [execution starting-execution
              input     input]
         (let [[stop? next-execution] (step-execution! cofx fx state-machine execution input)
               _                      (reset! latest-execution execution)
               ;; TODO(jeff): save intermediate executions (under (map second transitions))
               tx                     (save-execution fx next-execution {:can-fail? (not stop?)})]
           (if stop?
             (if (:ok @tx)
               next-execution
               (throw (ex-info "Failed to save execution"
                               {:execution/id      (:execution/id execution)
                                :execution/version (:execution/version execution)
                                :tx                (dissoc @tx :exception)}
                               (:exception @tx))))
             (recur (do (deref tx 1000 ::timeout)
                        (:entity @(acquire-execution fx "linear" next-execution nil {:can-fail? true})))
                    nil)))))
      (catch Exception e
        (record-exception fx latest-execution e)))))

(defn- run-linear-execution-inconsistent
  "Runs an execution, saving its state into the database executions. Avoids
  using the execution queue by running executions to completion.

  Unlike fair execution, runs each execution to completion before looking at the
  next one. This minimizes context switching costs but can cause a long backlog
  of work if each execution isn't fast to complete.

  Context switches can be very expensive in polling-based infrastructure state
  transition (assuming we wait for response to ensure write acknowledgements).
  "
  [fx state-machine starting-execution input]
  (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
        latest-execution (atom starting-execution)
        cofx             (make-cofx fx io)]
    (try
      (timed-run
       timeout
       (loop [execution starting-execution
              input     input]
         (let [[stop? next-execution] (step-execution! cofx fx state-machine execution input)
               _                      (reset! latest-execution execution)
               ;; TODO(jeff): save intermediate executions (under (map second transitions))
               tx                     (save-execution fx next-execution {:can-fail? (not stop?)})]
           (if stop?
             (if (:ok @tx)
               next-execution
               (throw (ex-info "Failed to save execution"
                               {:execution/id      (:execution/id execution)
                                :execution/version (:execution/version execution)
                                :tx                (dissoc @tx :exception)}
                               (:exception @tx))))
             (recur (do (acquire-execution fx "linear-inconsistent" next-execution nil {:can-fail? true})
                        next-execution)
                    nil)))))
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
  (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
        latest-execution (atom execution)
        cofx             (make-cofx fx io)]
    (try
      (timed-run
       timeout
       (let [[stop? next-execution] (step-execution! cofx fx state-machine execution input)
             _                      (reset! latest-execution execution)
             ;; TODO(jeff): save intermediate executions (under (map second transitions))
             tx                     @(save-execution fx next-execution {:can-fail? (not stop?)})]
         (if stop?
           (if (:ok tx)
             next-execution
             (throw (ex-info "Failed to save execution"
                             {:execution/id      (:execution/id execution)
                              :execution/version (:execution/version execution)
                              :tx                (dissoc @tx :exception)}
                             (:exception @tx))))
           (do
             (enqueue-execution fx (:execution/id execution) nil)
             next-execution))))
      (catch Exception e
        (record-exception fx latest-execution e)))))

(defn- sync-run
  ([fx state-machine execution] (sync-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (save-execution fx execution {:can-fail? false})
   (let [final-execution (run-sync-execution fx "synchronous-execution" state-machine execution input)]
     [(= "finished" (:execution/pause-state final-execution)) (:execution/id execution) final-execution])))
(defn- async-run
  ([fx state-machine execution] (async-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (let [ok (:ok @(save-execution fx execution {:can-fail? false}))]
     (when ok (enqueue-execution fx (:execution/id execution) input))
     [(boolean ok) (when ok (:execution/id execution)) nil])))

(defmethod executor :default [mode] {:error ::unknown-executor :value mode})
(defmethod executor "sync" [_] {:begin sync-run :step sync-run})
(defmethod executor "async-latency-unsafe" [_] {:begin async-run :step run-fair-execution}) ;; WARN(jeff): can have out-of-order execution
(defmethod executor "async-throughput" [_] {:begin async-run :step run-linear-execution})
(defmethod executor "async-throughput-inconsistent" [_] {:begin async-run :step run-linear-execution-inconsistent})

(defn create-execution-handler
  "Create an execution handler that processes executions.

  This includes:
   - Understanding :state-machine/execution-mode
   - Being idempotent for repeated executions are stored
  "
  ([fx] (create-execution-handler fx nil))
  ([fx {:keys [executor-name]}]
   (fn handler [eid input]
     (try
       (let [executor-name (or executor-name (.getName (Thread/currentThread)))
             [execution input]
             (loop [attempts 1]
               (when (< attempts 30)
                 (when-let [e (fetch-execution fx eid :latest)]
                   (when-not (and (= "finished" (:execution/pause-state e))
                                  (empty? (:execution/pending-effects e))
                                  (empty? (:execution/completed-effects e)))
                     ;;  (prn "ACQUIRE" (:execution/id e) (:execution/version e) (Thread/currentThread))
                     (when-let [res (acquire-execution fx executor-name e input {:can-fail? false})]
                       (if-let [e (:entity (deref res 10000 nil))]
                         [e input]
                         (do
                           (Thread/sleep (* attempts attempts))
                           (recur (inc attempts)))))))))]
         (when execution
           (let [state-machine  (fetch-statem fx (:execution/state-machine-id execution) (:execution/state-machine-version execution))
                 {:keys [step]} (executor (:execution/mode execution))]
             (step fx state-machine execution input))))
       (catch Throwable t
         (.printStackTrace t))))))

