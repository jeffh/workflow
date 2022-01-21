(ns net.jeffhui.workflow.api
  (:require [net.jeffhui.workflow.protocol :refer [sleep sleep-to enqueue-execution executor eval-action] :as protocol]
            [net.jeffhui.workflow.core :as core]
            [net.jeffhui.workflow.schema :as s]
            [net.jeffhui.workflow.tracer :as tracer]
            ;; for multimethod implementations
            net.jeffhui.workflow.io.http
            [clojure.string :as string]
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
  (tracer/with-span [sp "open"]
    (protocol/open obj)))
(defn close
  "Closes stateful connections for effects.

  Passthrough to [[protocol/close]]"
  [obj]
  (tracer/with-span [sp "close"]
    (protocol/close obj)))

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

(defn list-statem
  "Experimental: Returns a sequence of state machines."
  [fx options]
  (tracer/with-span [sp "list-statem"]
    (when-let [s (protocol/list-statem fx options)]
      (doseq [sm s]
        (s/debug-assert-statem sm))
      s)))

(defn fetch-statem
  "Returns a specific version of a state machine.

  If version is `:latest`, then the persistence layer will return the state machine with the highest version.
  "
  [fx statem-id statem-version]
  (tracer/with-span [sp "fetch-statem"]
    (when-let [s (protocol/fetch-statem fx statem-id statem-version)]
      (s/debug-assert-statem s))))

(defn list-executions
  "Experimental: Returns a sequence of executions."
  [fx options]
  (tracer/with-span [sp "list-executions"]
    (when-let [s (protocol/list-executions fx options)]
      (doseq [e s]
        (s/debug-assert-execution e))
      s)))

(defn fetch-execution
  "Returns a specific version of an execution.

  A version of an execution represents a specific moment in time of an execution.

  If version is `:latest`, then the persistence layer will return the execution with the highest version.
  "
  [fx execution-id execution-version]
  (tracer/with-span [sp "fetch-execution"]
    (-> sp
        (tracer/set-attr-str "execution-id" (str execution-id))
        (tracer/set-attr-str "execution-version" (pr-str execution-version)))
    (when-let [e (protocol/fetch-execution fx execution-id execution-version)]
      (s/debug-assert-execution e))))

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

(defn make-error
  "Returns an execution error map.

  This is used by protocol implementations to emit errors."
  ([category code reason]
   {:code     code
    :category category
    :reason   reason})
  ([category code reason extra]
   (merge {:code     code
           :category category
           :reason   reason}
          extra)))

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

   Returns a (future {:ok bool, :value {saved-state-machine...}, :error {...}})"
  [persistence statem]
  (tracer/with-span [sp "save-statem"]
    (let [errs (s/err-for-statem statem)]
      (if errs
        (do
          (tracer/set-attr-str sp "error" (pr-str errs))
          (future {:ok    false
                   :error errs}))
        (protocol/save-statem persistence statem nil)))))

;; (def save-execution protocol/save-execution) ;; This isn't common enough to put in this namespace
(defn- save-execution [persistence execution options]
  (tracer/with-span [sp "save-execution"]
    (let [res (protocol/save-execution persistence (s/debug-assert-execution execution) options)]
      (if (future? res)
        (future (let [ret @res]
                  (tap> {::type :save-execution ::execution execution ::options options ::ok (:ok ret) ::error (:error ret)})
                  ret))
        (do
          (tap> {::type :save-execution ::execution execution ::options options ::result res})
          res)))))

(defn effects [{:keys [statem execution scheduler interpreter exception-handler]}]
  {:pre [statem execution scheduler interpreter]}
  (protocol/->Effects statem execution scheduler interpreter exception-handler))

(defn- verify-execution [e]
  (assert (or (not (contains? e :execution/state))
              (:execution/state e)))
  e)

(defn io
  "Processes external io events. Also takes in consideration the any execution overrides.

  To invoke this function outside of a state machine, please use [[with-effects]]:
    (with-effects fx state-machine execution
      (io \"my-op\" :my :arguments :here))

  To invoke this function within a state machine, this can only be called under
  :invoke :call actions.

  To extend io, please require protocol/io and add a multimethod.

  io will always return a map that contains at least one key:

    {:ok bool} -> which indicates if the io succeeded or failed
  "
  [op & args]
  (tracer/with-span [sp "io"]
    (tracer/set-attr-str sp "op" (pr-str op))
    (tap> {::type :invoke-io ::io op :args args})
    (-> (if-let [code (get (:execution/io *execution*) op)]
          (let [res (eval-action code *fx* io (or (:execution/ctx *execution*)
                                                  (:execution/memory *execution*))
                                 nil)]
            (if (fn? res)
              (apply res args)
              res))
          (apply protocol/io op args))
        (s/assert-edn (format "(io %s ...) must return edn, but didn't" (pr-str op)))
        (s/assert-map (format "(io %s ...) must return a map, but didn't" (pr-str op)))
        (s/assert-errorable (format "(io %s ...) must return a map containing :error, but didn't" (pr-str op))))))

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

  Returns one of the following values:
   {:ok           true
    :execution/id #uuid \"...\"
    :execution    {...} | nil} ;; execution is returned if synchronous
   {:ok    false
    :error {:code ..., :category ..., ...}}

  WARNING: It isn't safe to for input to come from untrusted sources.
   - Recommended: validate input values before passing through this function.
   - Less Good Alternative: pass the untrusted input as a key in input.
  "
  ([fx state-machine-id]       (start fx state-machine-id (UUID/randomUUID) nil))
  ([fx state-machine-id input] (start fx state-machine-id (UUID/randomUUID) input))
  ([fx state-machine-id execution-id input]
   (tracer/with-span [sp "start"]
     (-> sp
         (tracer/set-attr-str "state-machine-id" (str state-machine-id))
         (tracer/set-attr-str "execution-id" (str execution-id))
         (tracer/set-attr-str "input" (pr-str input)))
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
             error        {:ok false :error error}
             (nil? begin) {:ok false :error (make-error :invalid-executor :execution/start "executor failed to provide begin")}
             :else        (begin fx state-machine execution input))))
       {:ok    false
        :error (make-error :state-machine-not-found :execution/start "state machine not found"
                           {:requested-statem-id state-machine-id})}))))

(defn trigger
  "Triggers a state machine to follow a specific transition.

  Note: It isn't safe to for input to come from untrusted sources.
   - Recommended: validate input values before passing through this function.
   - Less Good Alternative: pass the untrusted input as a key in input.

  If input includes:
    ::reply? true
  Then a clojure core/async channel is returned that will receive the execution.
  Otherwise, nil is returned.
  "
  [fx execution-id input]
  (tracer/with-span [sp "trigger"]
    (tracer/set-attr-str sp "execution-id" (str execution-id))
    (tracer/set-attr-str sp "input" (pr-str input))
    (when-not (::action input)
      (throw (IllegalArgumentException. (format "::action is missing from input: %s" (pr-str input)))))
    (if-let [execution (s/debug-assert-execution (fetch-execution fx execution-id :latest))]
      (enqueue-execution fx (:execution/id execution) input)
      (throw (ex-info "execution not found" {:execution/id execution-id})))))

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
  (tracer/with-span [sp "acquire-execution"]
    (-> sp
        (tracer/set-attr-str "executor-name" (str executor-name))
        (tracer/set-attr-str "executor-id" (str (:execution/id execution)))
        (tracer/set-attr-long "executor-version" (:execution/version execution)))
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
                      options))))

(defn- record-exception [fx starting-execution e]
  (let [err          (Throwable->map e)
        max-attempts 3]
    (protocol/report-error fx e)
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
(defn- save-transitions [fx stop? result]
  (tap> {::type :save-transitions ::transitions (:transitions result)})
  (let [opt {:can-fail? (not stop?)}]
    (if (seq (:transitions result))
      (last
       (for [t     (:transitions result)
             :let  [action (:id t)
                   e (:execution (meta t))]
             :when e]
         (let [f (save-execution fx (assoc e :execution/last-action action) opt)]
           (deref f 1 ::timeout) ;; here to induce partial ordering
           f)))
      (save-execution fx (:execution result) opt))))

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
  (tracer/with-span [sp "run-sync-execution"]
    (assert (= (:state-machine/id state-machine) (:execution/state-machine-id starting-execution))
            (pr-str (:state-machine/id state-machine) (:execution/state-machine-id starting-execution)))
    (let [timeout          (or (:state-machine/timeout-msec state-machine) sync-timeout)
          latest-execution (atom starting-execution)
          cofx             (make-cofx fx io)]
      (try
        (timed-run
         timeout
         (tracer/with-span [sp [sp "timed-run"]]
           (loop [execution starting-execution
                  input     input]
             (let [result         (step-execution! cofx fx state-machine execution input)
                   stop?          (core/result-stopped? result)
                   next-execution (:execution result)
                   _              (reset! latest-execution execution)
                   tx             (save-transitions fx stop? result)
                 ;; tx             (save-execution fx next-execution {:can-fail? (not stop?)})
                   ]
               (if stop?
                 (if (:ok @tx)
                   next-execution
                   (throw (ex-info "Failed to save execution"
                                   {:execution/id      (:execution/id execution)
                                    :execution/version (:execution/version execution)
                                    :tx                @tx})))
                 (recur (:value @(acquire-execution fx executor-name next-execution nil {:can-fail? true}))
                        nil))))))
        (catch Throwable e
          (tap> {::type :execution-exception ::execution @latest-execution ::exception e})
          (tracer/record-exception sp e)
          (record-exception fx @latest-execution e))))))

(defmacro with-effects [fx state-machine execution & body]
  `(let [e# ~execution]
     (binding [*fx*            ~fx
               *state-machine* ~state-machine
               *execution*     e#
               *execution-id*  (:execution/id e#)]
       ~@body)))

(defn- should-stop? [execution effects error]
  (core/result-stopped? (core/->Result execution effects nil error)))

(defn- throwable->map [t]
  (tracer/with-span [sp "throwable->map"]
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
                (mapv scrub-err errs))))))

(defn- step-execution! [cofx fx state-machine execution input]
  ;; Advances the execution, running side effects if needed
  ;;
  ;; Pipeline: Trigger/Step -(1-to-many)-> Pending Effects -(1-to-many)-> Completed Effects
  ;;
  ;; Algorithm:
  ;;  1. Run any incoming input trigger
  ;;  2. Execute any pending effects
  ;;     TODO(jeff): we should process pending effects one at a time
  ;;  3. Process any completion effects
  ;;  4. Automate decision making
  (tracer/with-span [sp "step-execution!"]
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
                                                               (update :execution/pending-effects (comp not-empty (fnil into [])) effects)
                                                               (cond-> error (update :execution/version inc)))
            new-result                                     (assoc result :execution next-execution)]
        (cond-> new-result
          (core/result-stopped? new-result)
          (assoc-in [:execution :execution/finished-at] now)))

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

                                                             {execution-id :execution/id, :keys [ok execution error]}
                                                             (start fx state-machine-id execution-id input)]
                                                         (merge
                                                          {:error            (if ok nil
                                                                                 (or error
                                                                                     (make-error :execution/start :io "failed to start execution")))
                                                           :async?           async?
                                                           :state-machine/id state-machine-id
                                                           :execution/id     execution-id}
                                                          (when execution {:execution execution})))
                                      :execution/step (let [{:keys [execution-id action input async?]}
                                                            args

                                                            result
                                                            (trigger fx execution-id (merge input
                                                                                            {::action input}
                                                                                            (when async?
                                                                                              {::reply? true})))]
                                                        (when result
                                                          (async/alt!!
                                                            (async/timeout 10000) {:error (make-error :execution/step :io
                                                                                                      "waiting for result to trigger execution has timed out")}
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
                                                            (protocol/eval-action expr fx io (or (:execution/ctx execution)
                                                                                                 (:execution/memory execution))
                                                                                  input)
                                                            (catch Throwable t
                                                              {:error (make-error :invoke/io
                                                                                  :io
                                                                                  "io invocation threw exception"
                                                                                  {:input     input
                                                                                   :expr      expr
                                                                                   :throwable (throwable->map t)})})))
                                      :sleep/seconds    (let [{:keys [seconds]} args]
                                                          (protocol/sleep fx seconds (:execution/id execution)
                                                                          {::resume {:id     complete-ref
                                                                                     :return {:sleep seconds}}}))
                                      :sleep/timestamp  (let [{:keys [timestamp]} args]
                                                          (protocol/sleep-to fx timestamp (:execution/id execution)
                                                                             {::resume {:id     complete-ref
                                                                                        :return {:timestamp timestamp}}})))
                                    (catch Throwable t
                                      {:error (make-error :io/uncaught-exception :io
                                                          "unexpected exception thrown when invoking effect"
                                                          {:throwable (throwable->map t)})}))]
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
        (core/->Result (cond-> next-execution
                         stop? (assoc :execution/finished-at (now!)))
                       (:execution/pending-effects next-execution)
                       nil
                       (:execution/error next-execution)))

      ;; advance the state machine based on effects that have completed that the
      ;; execution wants to be notified about success/failure
      (:execution/completed-effects execution)
      (let [effect-result                                  (first (:execution/completed-effects execution))
            {:keys [effects transitions error] :as result} (core/step-execution cofx state-machine
                                                                                (assoc execution :execution/error nil)
                                                                                (set/rename-keys effect-result {::action ::core/action
                                                                                                                ::resume ::core/resume}))
            now                                            (now!)
            next-execution                                 (-> (:execution result)
                                                               (assoc :execution/error error
                                                                      :execution/pending-effects effects
                                                                      :execution/step-ended-at now
                                                                      :execution/comment "Processed effect result")
                                                               (update :execution/completed-effects (comp not-empty subvec) 1)
                                                               (cond-> error (update :execution/version inc)))
            stop?                                          (should-stop? next-execution effects error)]
        (core/->Result (cond-> next-execution
                         stop? (assoc :execution/finished-at now))
                       (:execution/pending-effects next-execution)
                       transitions
                       (:execution/error next-execution)))

      ;; advance possibly an automated state machine advance
      :else
      (let [{:keys [effects transitions error] :as result} (core/step-execution cofx state-machine (assoc execution :execution/error nil)
                                                                                (set/rename-keys input {::action ::core/action
                                                                                                        ::resume ::core/resume}))
            now                                            (now!)
            next-execution                                 (-> (:execution result)
                                                               (assoc
                                                                :execution/error error
                                                                :execution/pending-effects effects
                                                                :execution/step-ended-at now
                                                                :execution/comment "Processed step")
                                                               (cond-> error (update :execution/version inc)))
            stop?                                          (should-stop? next-execution effects error)]
        (core/->Result (cond-> next-execution
                         stop? (assoc :execution/finished-at now))
                       (:execution/pending-effects next-execution)
                       transitions
                       (:execution/error next-execution))))))

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
  (tracer/with-span [sp "run-linear-execution"]
    (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
          latest-execution (atom starting-execution)
          cofx             (make-cofx fx io)]
      (try
        (timed-run
         timeout
         (tracer/with-span [sp [sp "timed-run"]]
           (loop [execution starting-execution
                  input     input]
             (let [result         (step-execution! cofx fx state-machine execution input)
                   stop?          (core/result-stopped? result)
                   next-execution (:execution result)
                   _              (reset! latest-execution execution)
                   tx             (save-transitions fx stop? result)
                 ;; tx             (save-execution fx next-execution {:can-fail? (not stop?)})
                   ]
               (if stop?
                 (if (:ok @tx)
                   next-execution
                   (throw (ex-info "Failed to save execution"
                                   {:execution/id      (:execution/id execution)
                                    :execution/version (:execution/version execution)
                                    :tx                (dissoc @tx :exception)}
                                   (:exception @tx))))
                 (recur (do (deref tx 1000 ::timeout)
                            (:value @(acquire-execution fx "linear" next-execution nil {:can-fail? true})))
                        nil))))))
        (catch Throwable e
          (tap> {::type :execution-exception ::execution @latest-execution ::exception e})
          (tracer/record-exception sp e)
          (record-exception fx @latest-execution e))))))

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
  (tracer/with-span [sp "run-linear-execution-inconsistent"]
    (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
          latest-execution (atom starting-execution)
          cofx             (make-cofx fx io)]
      (try
        (timed-run
         timeout
         (tracer/with-span [sp [sp "timed-run"]]
           (loop [execution starting-execution
                  input     input]
             (let [result         (step-execution! cofx fx state-machine execution input)
                   stop?          (core/result-stopped? result)
                   next-execution (:execution result)
                   _              (reset! latest-execution execution)
                   tx             (save-transitions fx stop? result)
                 ;; tx             (save-execution fx next-execution {:can-fail? (not stop?)})
                   ]
               (if stop?
                 (if (:ok @tx)
                   next-execution
                   (throw (ex-info "Failed to save execution"
                                   {:execution/id      (:execution/id execution)
                                    :execution/version (:execution/version execution)
                                    :tx                (dissoc @tx :exception)}
                                   (:exception tx))))
                 (recur (do (acquire-execution fx "linear-inconsistent" next-execution nil {:can-fail? true})
                            next-execution)
                        nil))))))
        (catch Throwable e
          (tap> {::type :execution-exception ::execution @latest-execution ::exception e})
          (tracer/record-exception sp e)
          (record-exception fx @latest-execution e))))))

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
  (tracer/with-span [sp "run-fair-execution"]
    (assert (:execution/state-machine-id execution))
    (assert (:execution/state-machine-version execution))
    (let [timeout          (or (:state-machine/timeout-msec state-machine) async-step-timeout)
          latest-execution (atom execution)
          cofx             (make-cofx fx io)]
      (try
        (timed-run
         timeout
         (tracer/with-span [sp [sp "timed-run"]]
           (let [result         (step-execution! cofx fx state-machine execution input)
                 stop?          (core/result-stopped? result)
                 next-execution (:execution result)
                 _              (reset! latest-execution execution)
                 tx             (save-transitions fx stop? result)
               ;; tx             @(save-execution fx next-execution {:can-fail? (not stop?)})
                 ]
             (if stop?
               (if (:ok @tx)
                 next-execution
                 (throw (ex-info "Failed to save execution"
                                 {:execution/id      (:execution/id execution)
                                  :execution/version (:execution/version execution)
                                  :tx                (dissoc @tx :exception)}
                                 (:exception @tx))))
               (do
                 (enqueue-execution fx (:execution/id execution) nil)
                 next-execution)))))
        (catch Throwable e
          (tap> {::type :execution-exception ::execution @latest-execution ::exception e})
          (tracer/record-exception sp e)
          (record-exception fx @latest-execution e))))))

(defn- sync-run
  ([fx state-machine execution] (sync-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (tracer/with-span [sp "sync-run"]
     (save-execution fx execution {:can-fail? false})
     (let [final-execution (run-sync-execution fx "synchronous-execution" state-machine execution input)]
       {:ok           (= "finished" (:execution/pause-state final-execution))
        :execution/id (:execution/id execution)
        :execution    final-execution}))))
(defn- async-run
  ([fx state-machine execution] (async-run fx state-machine execution nil))
  ([fx state-machine execution input]
   (tracer/with-span [sp "async-run"]
     (let [ok (:ok @(save-execution fx execution {:can-fail? false}))]
       (when ok (enqueue-execution fx (:execution/id execution) input))
       {:ok           (boolean ok)
        :execution/id (when ok (:execution/id execution))
        :execution    nil}))))

(defmethod executor :default [mode] {:error (make-error ::unknown-executor :state-machine-definition (format "%s is not a valid execution-mode" mode))
                                     :value mode})
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
     (tracer/with-span [sp "execution-handler.handle"]
       (tracer/set-attr-str sp "execution-id" eid)
       (tracer/set-attr-str sp "input" (pr-str input))
       (try
         (let [executor-name (or executor-name (.getName (Thread/currentThread)))
               [execution input]
               (loop [attempts 1]
                 (when (< attempts 30)
                   (tracer/add-event sp "attempt")
                   (when-let [e (fetch-execution fx eid :latest)]
                     (when-not (and (= "finished" (:execution/pause-state e))
                                    (empty? (:execution/pending-effects e))
                                    (empty? (:execution/completed-effects e)))
                         ;;  (prn "ACQUIRE" (:execution/id e) (:execution/version e) (Thread/currentThread))
                       (when-let [res (acquire-execution fx executor-name e input {:can-fail? false})]
                         (if-let [e (:value (deref res 10000 nil))]
                           [e input]
                           (do
                             (tracer/with-span [sp [sp "sleep"]]
                               (let [ms (* attempts attempts)]
                                 (tracer/set-attr-long sp "ms" ms)
                                 (Thread/sleep ms)))
                             (recur (inc attempts)))))))))]
           (when execution
             (let [state-machine  (fetch-statem fx (:execution/state-machine-id execution) (:execution/state-machine-version execution))
                   {:keys [step]} (executor (:execution/mode execution))]
               (tracer/with-span [sp [sp "executor"]]
                 (step fx state-machine execution input)))))
         (catch Throwable t
           (tracer/record-exception sp t)
           (protocol/report-error fx t)
           (.printStackTrace t)))))))

(defn child-execution-ids
  "Helper function. Returns execution ids of just-created child executions.

  No arguments returns the transducer function.

  Example:

    (distinct (keep child-execution-ids (fetch-execution-history fx my-execution-id)))
  "
  ([]
   (comp
    (map ::resume)
    (filter (comp #{:execution/start} :op))
    (map (comp :execution/id :return))))
  ([execution]
   (not-empty (sequence (child-execution-ids) (:execution/completed-effects execution)))))

(defn explain-execution-history
  "Experimental. Returns a more human-friendly output of what happened in an execution.

  Returns an informal sequence of:

   (with-meta
     [execution-version event & data]
     {:description       \"some basic description of the event\"
      :ctx-changes       {key [old-value '=> new-value]}
      :execution-changes {key [old-value '=> new-value]}
      :execution         ...})
  "
  [fx execution-id]
  (let [history       (fetch-execution-history fx execution-id)
        ;; statem        (fetch-statem fx
        ;;                             (:execution/state-machine-id (first history))
        ;;                             (:execution/state-machine-version (first history)))
        resume-id->fx (atom {})
        changes       (fn [m1 m2 no-changes]
                        (or
                         (not-empty
                          (into {}
                                (keep identity
                                      (for [k    (into (set (keys m1)) (keys m2))
                                            :let [v1 (get m1 k ::missing)
                                                  v2 (get m2 k ::missing)]]
                                        (when (not= v1 v2)
                                          [k [(if (= v1 ::missing)
                                                'missing-key
                                                v1) '=>
                                              (if (= v2 ::missing)
                                                'missing-key
                                                v2)]])))))
                         no-changes))]
    (doall
     (mapcat
      identity
      (for [[e prev] (map vector history (into [nil] history))
            :let     [evt (fn [desc & values]
                            (with-meta (into [(:execution/version e)] (keep identity values))
                              {:description       desc
                               :ctx-changes       (changes (:execution/ctx prev) (:execution/ctx e) nil)
                               :execution-changes (changes prev e nil)
                               :execution         e}))]]
        (cond
          (nil? prev) [(evt "execution was created from a state machine"
                            :created (select-keys e [:execution/state-machine-id
                                                     :execution/state-machine-version
                                                     :execution/id
                                                     :execution/input]))]

          (:execution/pending-effects e) (concat (when (not= (:execution/state prev) (:execution/state e))
                                                   [(evt "execution completed an action that changed the execution state, prior to effects completed"
                                                         :change-state {:to   (:execution/state e)
                                                                        :from (:execution/state prev)})])
                                                 (mapv #(let [resume-id (ffirst (:resumers (:execution/pause-memory e)))
                                                              form      (condp = (:op %1)
                                                                          :invoke/io        (with-meta
                                                                                              (let [expr (:expr (:args %1))]
                                                                                                (if (> (count expr) 2)
                                                                                                  (into '(...) (reverse (take 2 (:expr (:args %1)))))
                                                                                                  expr))
                                                                                              {:expr (:expr (:args %1))})
                                                                          :execution/start  (with-meta
                                                                                              `(~'start
                                                                                                ~(:state-machine-id (:args %1))
                                                                                                ...)
                                                                                              {:state-machine-id (:state-machine-id (:args %1))
                                                                                               :execution-id     (:execution-id (:args %1))
                                                                                               :input            (:input (:args %1))})
                                                                          :execution/return (if-let [id (:to (:args %1))]
                                                                                              [:return (:result (:args %1))]
                                                                                              [:return])
                                                                          [(:op %1) (:args %1)])]
                                                          (swap! resume-id->fx assoc resume-id form)
                                                          (evt (condp = (:op %1)
                                                                 :invoke/io     "action requests an io side effect to occur and receive the result"
                                                                 :invoke/start  "action requests an io side effect to occur and receive the result"
                                                                 :invoke/return "action requests to notify the completion of this execution to another execution"
                                                                 "action requests a side effect to occur")
                                                               :requested-effect form))
                                                       (:execution/pending-effects e)))

          (> (count (:execution/completed-effects e))
             (count (:execution/completed-effects prev)))
          [(#(let [r (::resume %)]
               (evt
                (if (:ok (:return r))
                  "a side effect has completed successfully and is pending to be processed by the state machine execution"
                  "a side effect has failed to complete, and is pending to be processed by the state machine execution")
                (if (:ok (:return r))
                  :effect-succeeded
                  :effect-failed)
                (get @resume-id->fx (:id r) r)
                (:return r)))
            (first (:execution/completed-effects e)))]
          ;; This technically shouldn't be any different from normal state transition below
          ;; (< (count (:execution/completed-effects e))
          ;;    (count (:execution/completed-effects prev)))
          ;; [(#(let [r (::resume %)]
          ;;      (evt (if (:ok (:return r))
          ;;             :react-effect-succeeded
          ;;             :react-effect-failed)
          ;;           (get @resume-id->fx (:id r) r)
          ;;           (:return r)))
          ;;   (first (:execution/completed-effects e)))]

          (not= (:execution/state prev) (:execution/state e))
          [(evt "the action/transition being followed"
                :action (:execution/last-action e))
           (evt "a pure state transition of the state machine from one state to another"
                :change-state {:to     (:execution/state e)
                               :from   (:execution/state prev)})]

          (and (:action-id (:execution/pause-memory e))
               (not= "wait-fx" (:execution/pause-state prev)))
          [(evt (if (#{"wait-fx"} (:execution/pause-state prev))
                  "the action/transition being followed to process the completed side effects"
                  "the action/transition being followed that has side effects to enqueue")
                (if (#{"wait-fx"} (:execution/pause-state prev))
                  :resuming-action
                  :action)
                (:action-id (:execution/pause-memory e)))]

          (clojure.string/includes? (:execution/comment e) "Resuming") nil

          (= "finished" (:execution/pause-state e))
          [(evt "the execution has completed"
                :finished {:state          (:execution/state e)
                           :duration       [(/ (- (:execution/finished-at e) (:execution/started-at e)) 1e9) :ms]
                           :estimated-size [(count (pr-str history)) :bytes]})]

          :else [(evt "an unrecognized pattern of state machine execution"
                      :unknown e)]))))))
