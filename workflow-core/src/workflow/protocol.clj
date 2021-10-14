(ns workflow.protocol
  (:import java.util.Date
           java.time.Instant
           java.util.concurrent.Executors
           java.util.concurrent.ThreadFactory
           java.util.concurrent.ScheduledExecutorService
           java.util.concurrent.TimeUnit))

(defn ^ThreadFactory thread-factory
  ([namer-fn] (thread-factory namer-fn nil))
  ([namer-fn {:keys [daemon?]}]
   (let [i (atom 1)]
     (reify ThreadFactory
       (newThread [_ r]
         (let [t (Thread. r (namer-fn (swap! i inc)))]
           (when-not (nil? daemon?) (.setDaemon t (boolean daemon?)))
           t))))))

(defonce ^:private local-scheduler
  (delay (Executors/newScheduledThreadPool 1 (thread-factory (constantly "wf-local-scheduler") {:daemon? true}))))

(defn schedule-recurring-local-task! [initial-ms interval-ms f]
  (let [fut (.scheduleAtFixedRate ^ScheduledExecutorService @local-scheduler f (long initial-ms) (long interval-ms) TimeUnit/MILLISECONDS)]
    (fn cancel [] (.cancel fut false))))

(defprotocol Connection
  (open* [_] "Sets up the instance")
  (close* [_] "Shuts down the instance"))

(defn open [obj] (if (satisfies? Connection obj) (open* obj) obj))
(defn close [obj] (if (satisfies? Connection obj) (close* obj) obj))

(defprotocol MachineInterpreter
  (evaluate-expr [_ expr io state input output]
    "Evaluates an expression and returns an action map for workflow to process.

     output := ::nothing if there is no output value."))

(defprotocol StateMachinePersistence
  ;; Notes for persistence implementations:
  ;;  - state-machine-id+version should always be unique, immutable, & accumulative
  ;;  - state-machine-id is an arbitrary string
  (fetch-statem [_ state-machine-id version]
    "Returns a specific version of a state machine. Fetch latest if version = :latest")
  (save-statem [_ state-machine]
    "Saves a specific version a state machine.
	 Returns a future of {:ok bool, :entity {saved-state-machine...}}"))

(defprotocol ExecutionPersistence
  ;; Notes for persistence implementations:
  ;;  - execution-id+version should always be unique, immutable, & accumulative
  ;;  - execution-id is always a UUID
  (executions-for-statem [_ state-machine-id options]
    "Returns a sequence of executions by a given state machine. Ordered by latest executions started-at.

		options - {:limit int, :offset int, :version #{:latest, int, :all}}")
  (fetch-execution [_ execution-id version]
    "Returns a specific version of an execution. Fetch latest if version = :latest")
  (fetch-execution-history [_ execution-id]
    "Returns a sequence of the full history of an execution, ordered by execution history (creation is first)")
  (save-execution [_ execution]
    "Saves a new version of an execution.
     Returns a future of {:ok bool, :entity {saved-execution...}}"))

;; Optional protocol that some schedulers may use to outsource their ability to schedule task in the future
(defprotocol SchedulerPersistence
  (save-task [_ timestamp execution-id options] "returns task id or nil on failure")
  (runnable-tasks [_ now] "returns a seq of tasks to run {:task/id, :task/response, :task/execution-id, :task/execution-options, :task/start-after}")
  (complete-task [_ task-id reply] "marks a saved task as complete, giving a return value for an observer"))

(defprotocol Scheduler
  (sleep-to [_ timestamp execution-id options]
    "Schedules the given execution to be enqueued after a specific
     datetime. Return value is truthy on successfully scheduling.

    timestamp :- #inst / Date
    ")
  (enqueue-execution [_ execution-id options]
    "Enqueues an execution to resume execution. Returns a core.async/chan if a reply is expected.

     Parameters:
	   execution - the execution-id to run.
	   options - {::workflow/reply? bool
	              :as args}
          Represents the input for resuming the execution. reply? indicates that a
          channel should be returned of the state of the execution on completion
          (can be successful or failure).")
  (register-execution-handler [_ f]
    "Registers f to process executions. f = nil means to unregister.

    Implementations are expected to set up connection work necessary to process
    work within this method.

    f must be idempotent.
    f :- (fn [execution-id :- uuid?, input :- map?] :- reply-value)"))

(defn sleep
  "Schedules the given execution to be enqueued after a certain amount of
   time has elapsed. Return value is truthy on successfully scheduling.

   duration is in milliseconds, and only ensure the execution happens after the elasped duration."
  [schedule-persistence duration-ms execution-id options]
  (sleep-to schedule-persistence (Date/from (.plusMillis (Instant/now) duration-ms)) execution-id options))

(defmulti executor (fn [kind] kind))
(defmulti io (fn [op & _args] op))
(defmethod io :default [name & _args]
  (throw (IllegalArgumentException. (format "io does not support '%s'" name))))

(defrecord Effects [state-machine-persistence execution-persistence scheduler interp]
  Connection
  (open* [this]
    (assoc this
           :state-machine-persistence (open state-machine-persistence)
           :execution-persistence (open execution-persistence)
           :scheduler (open scheduler)
           :interp (open interp)))
  (close* [this]
    (assoc this
           :interp (close interp)
           :scheduler (close scheduler)
           :execution-persistence (close execution-persistence)
           :state-machine-persistence (close state-machine-persistence)))
  StateMachinePersistence
  (fetch-statem [_ state-machine-id version] (fetch-statem state-machine-persistence state-machine-id version))
  (save-statem [_ state-machine] (save-statem state-machine-persistence state-machine))
  ExecutionPersistence
  (executions-for-statem [_ state-machine-id options] (executions-for-statem execution-persistence state-machine-id options))
  (fetch-execution [_ execution-id version] (fetch-execution execution-persistence execution-id version))
  (fetch-execution-history [_ execution-id] (fetch-execution-history execution-persistence execution-id))
  (save-execution [_ execution] (save-execution execution-persistence execution))
  Scheduler
  (sleep-to [_ timestamp execution-id options] (sleep-to scheduler timestamp execution-id options))
  (enqueue-execution [_ execution options] (enqueue-execution scheduler execution options))
  (register-execution-handler [_ f] (register-execution-handler scheduler f))
  MachineInterpreter
  (evaluate-expr [_ expr io context input output] (evaluate-expr interp expr io context input output)))

(defn eval-action
  ([expr fx io context input] (evaluate-expr fx expr io context input ::nothing))
  ([expr fx io context input output] (evaluate-expr fx expr io context input output)))
