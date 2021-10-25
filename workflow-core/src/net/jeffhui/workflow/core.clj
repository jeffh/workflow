(ns net.jeffhui.workflow.core
  (:import java.util.UUID)
  (:require [clojure.string :as string]
            [clojure.set :as set]))

;; TODO(jeff): Error should show code snippet executed along with exception

#_
(defn debug-print [& values]
  (locking *out*
    (prn values)))

(defmacro ^:private assert-arg [expr msg & args]
  `(when-not ~expr
     (throw (IllegalArgumentException. (format ~msg ~@args)))))

(defn empty-execution
  "Creates an execution that has some initial values. These are fundamental
   values needed to step through an execution.

  Caller is expected to evaluate :initial-context key
  "
  [{:keys [execution-id state-machine initial-context input return-to]}]
  {:execution/id                    execution-id
   :execution/version               1
   :execution/state-machine-id      (:state-machine/id state-machine)
   :execution/state-machine-version (:state-machine/version state-machine)
   :execution/state                 (:state-machine/start-at state-machine)
   :execution/pause-state           "ready"
   :execution/memory                initial-context
   :execution/input                 input
   :execution/pause-memory          nil
   :execution/return-to             (or return-to (::return-to input))})

(defrecord Result [execution effects transitions error])

(defmacro ^:private try? [execution action body]
  `(try
     ~body
     (catch Throwable t#
       (->Result ~execution
                 nil
                 nil
                 #:error{:code      :exception
                         :reason    "Exception thrown when evaluating code"
                         :action    ~action
                         :throwable (Throwable->map t#)}))))

(declare next-execution)
(defn step-execution
  "Makes one logical step of the execution. This means the execution runs until
   it needs to run an effect or needs to wait for input.

  Types:
    Returns {:execution next-execution, :effects [{:op :kw, :args ..., :complete-ref resume-id}], :transitions [...], :error ...}
      next-execution will have pause-state of #{\"ready\" \"wait\" \"await-input\" \"finished\"}
        - \"ready\" indicates next-execution can be called again, it may or may
          not advance based on conditions of the actions.
        - \"await-input\" indicates the execution requires input to advance.
          This typically means providing an ::action key
        - \"wait\" indicates this execution is waiting for completion of the return effect(s).
          After effects run, the ::resume key should be provided with a vector of completions.
          ::resume => {:id resume-id, :return fx-return-value}
        - \"finished\" indicates this execution has terminated. Futher attempts
          to step will be unchanged.

      effects are a vector of side effects to initiate. They may optionally have
        a complete-ref if the execution is awaiting for a return value for the given
        effect.

        NOTE: this contract is unstable and may change from version to version

        Current ops that are required:
          :execution/start  {:state-machine-id .., :execution-id ..., :input ..., async? bool} -> {:ok bool, :execution/id ...}
          :execution/step   {:execution-id ..., :action ... :input ..., :async? Maybe(bool)} -> {:ok bool, ...}
          :execution/return {...} -> Void
          :invoke/io        {:input ..., :expr ...} -> ...
          :sleep/seconds    {:seconds Int} -> ...
          :sleep/timestamp  {:timestamp Inst} -> ...

      transitions are the sequence of [action-id execution] that indicates what
        path was taken produces the associated execution.

        Also, each element will have metadata that indicates the execution at
        this point in time.

    Cofx:
      eval-expr!           => Code context input -> EDN
      eval-expr!           => Code context input output -> EDN
      current-time!        => Void -> Int
      random-resume-id!    => Void -> EDN
      random-execution-id! => Void -> EDN

    Special Input Keys:
     ::action => keyword | string
          Indicates a specific action/transition to follow
     ::resume => {:id resume-id, :return fx-return-value}
          Indicates a specific effect requested by the execution has completed with a given value
  "
  ([cofx state-machine execution] (step-execution cofx state-machine execution nil))
  ([cofx state-machine execution input]
   (loop [prev-result        nil
          result             (with-meta (->Result execution nil nil nil) {:previous execution})
          input              input
          result-transitions (transient [])]
     (let [{:keys [execution effects transitions error]} result]
       (if (and prev-result (or effects error
                                (#{"wait" "finished"} (:execution/pause-state execution))
                                (= (:execution/version (:execution prev-result))
                                   (:execution/version execution))))
         (if (:error/rejected? error)
           (assoc prev-result :transitions (persistent! result-transitions))
           (assoc result :transitions (persistent! (do (doseq [t transitions]
                                                         (conj! result-transitions (with-meta t {:execution execution
                                                                                                 :effects   effects
                                                                                                 :error     error})))
                                                       result-transitions))))
         (recur result
                (next-execution cofx state-machine execution input)
                nil
                (do (doseq [t transitions]
                      (conj! result-transitions (with-meta t {:execution execution
                                                              :effects   effects
                                                              :error     error})))
                    result-transitions)))))))

(defn- requires-trigger? [statem-states state]
  (boolean
   (let [state-node (get statem-states state)]
     (and state-node (some (comp string? :when) (:actions state-node))))))
(defn- ready-or-await-pause-state [statem-states state]
  (if (requires-trigger? statem-states state)
    "await-input"
    "ready"))

(defn- next-ver [execution] (update execution :execution/version inc))
(defn next-execution
  "Performs exactly one step of execution. Returns the next :execution state and :effects to run. Failure fills an :error key.

  Typically, EXACTLY ONE step is undesirable. Use [[step-execution]] instead.

  Types:
    Returns {:execution next-execution, :effects [{:op :kw, :args ..., :complete-ref resume-id}], :error ...}
      next-execution will have pause-state of #{\"ready\" \"wait\" \"await-input\"}
        - \"ready\" indicates next-execution can be called again, it may or may
          not advance based on conditions of the actions.
        - \"await-input\" indicates the execution requires input to advance.
          This typically means providing an ::action key
        - \"wait\" indicates this execution is waiting for completion of the return effect(s).
          After effects run, the ::resume key should be provided with a vector of completions.
          ::resume => {:id resume-id, :return fx-return-value}
        - \"finished\" indicates this execution has terminated. Futher attempts
          to step will be unchanged.

      effects are a vector of side effects to initiate. They may optionally have
        a complete-ref if the execution is awaiting for a return value for the given
        effect.

        NOTE: this contract is unstable and may change from version to version

        Current ops that are required:
          :execution/start  {:state-machine-id .., :execution-id ..., :input ..., async? bool} -> {:ok bool, :execution/id ..., :error ...}
          :execution/step   {:execution-id ..., :action Maybe(...) :input ..., :async? Maybe(bool)} -> {:ok bool, :error ..., ...}
          :execution/return {...} -> Void
          :invoke/io        {:input ..., :expr ...} -> {:ok bool, :error ..., :value ....}
          :sleep/seconds    {:seconds Int} -> {:ok bool, :error ...}
          :sleep/timestamp  {:timestamp Inst} -> {:ok bool, :error ...}

    Cofx:
      eval-expr!           => Code context input -> EDN
      eval-expr!           => Code context input output -> EDN
      current-time!        => Void -> Int
      random-resume-id!    => Void -> EDN
      random-execution-id! => Void -> EDN

    Special Input Keys:
     ::action => keyword | string
          Indicates a specific action/transition to follow
     ::resume => {:id resume-id, :return fx-return-value}
          Indicates a specific effect requested by the execution has completed with a given value
  "
  ([cofx state-machine execution] (next-execution cofx state-machine execution nil))
  ([{:keys [eval-expr! current-time! random-resume-id! random-execution-id!]} state-machine execution input]
   (let [action-name (::action input)
         resume      (::resume input)
         states      (:state-machine/states state-machine)
         state       (:execution/state execution)
         state-node  (get states state)
         actions     (:actions state-node)
         data        (:execution/memory execution)]
     (assert-arg state "Execution state cannot be nil: %s" (pr-str execution))
     (assert-arg state-node "Execution state not found in state machine: %s" (pr-str {:expected-state  state
                                                                                      :possible-states (keys states)}))
     #_(debug-print "STEP" (:execution/state-machine-id execution) (:execution/id execution) input)
     (vary-meta
      (if (contains? state-node :return)
         (if (= "finished" (:execution/pause-state execution))
           (->Result execution nil nil nil)
           (let [return-value (eval-expr! (:return state-node) data input)]
             (->Result (assoc (next-ver execution) :execution/pause-state "finished")
                       [{:op   :execution/return
                         :args {:result return-value
                                :to     (when-let [return-to (:execution/return-to execution)]
                                          [return-to])}}]
                       [{:id "finish" :finished true}]
                       nil)))
         (if (= "wait" (:execution/pause-state execution))
           (cond
             (not resume)
             (->Result execution nil nil
                       #:error{:rejected? true
                               :code      :no-resume
                               :reason    "Expect a resume action"
                               :input     input})

             (nil? (contains? (:resumers (:execution/pause-memory execution)) (:id resume)))
             (->Result execution nil nil
                       #:error{:rejected? true
                               :code      :invalid-resume-id
                               :reason    "Resuming execution requires a resume id"
                               :input     input})

             :else
             (try?
              execution
              (merge {:id      (:action-id (:execution/pause-memory execution))
                      :resume? true}
                     (get (:resumers (:execution/pause-memory execution)) (:id resume)))
              (let [rid (:id resume)

                    {:keys [action-id original-input resumers]} (:execution/pause-memory execution)
                    {:keys [then]}                              (get resumers rid)

                    next-state    (or (:state then) state)
                    next-data     (if (:context then)
                                    (eval-expr! (:context then) data original-input (:return resume))
                                    data)
                    next-resumers (not-empty (dissoc (:resumers (:execution/pause-memory execution)) rid))]
                #_(debug-print "RESUME" (:execution/state-machine-id execution) resume)
                (->Result (merge (next-ver execution)
                                 {:execution/state  next-state
                                  :execution/memory next-data}
                                 (if next-resumers
                                   {:execution/pause-state  "wait"
                                    :execution/pause-memory (assoc (:execution/pause-memory execution) :resumers next-resumers)}
                                   {:execution/pause-state  (ready-or-await-pause-state states next-state)
                                    :execution/pause-memory nil}))
                          nil
                          [{:id (str "returned(" action-id ")") :ref rid :return? true}]
                          nil))))
           (let [actions (vec actions)
                 size    (count actions)]
             (loop [i 0]
               (if (= i size)
                 (if (and (nil? action-name) (= "ready" (:execution/pause-state execution)))
                   (->Result (assoc (next-ver execution) :execution/pause-state "await-input") nil nil nil)
                   (->Result execution nil nil
                             #:error{:rejected?        true
                                     :code             :no-matched-actions
                                     :reason           "No actions matched requirements"
                                     :requested-action action-name
                                     :possible-actions (set (map #(select-keys % [:id :when]) actions))}))
                 (let [action (actions i)
                       wh     (:when action)]
                   (if (or (nil? wh)
                           (and (or (string? wh)
                                    (keyword? wh)) ;; match by input ::action
                                (= action-name wh))
                           (and (not (string? wh)) ;; match by expression
                                (eval-expr! wh data input)))
                     (cond
                       (:invoke action)
                       (let [invoke (:invoke action)]
                         (cond
                           (:state-machine invoke)
                           (let [async?           (boolean (:async? invoke))
                                 aid              (:id action)
                                 eid              (random-execution-id!)
                                 rid              (random-resume-id!)
                                 next-state       (or (:state action) state)
                                 next-data        (if (:context action)
                                                    (eval-expr! (:context action) data input)
                                                    data)
                                 state-machine-id (eval-expr! (:state-machine invoke) data input)
                                 statem-input     (merge (:input invoke)
                                                         (when-let [eio (:execution/io execution)] {::io eio})
                                                         (when-not async? {::return-to [(:execution/id execution) rid]}))]
                             (->Result (merge (next-ver execution)
                                              {:execution/pause-state  "wait"
                                               :execution/pause-memory {:action-id        aid
                                                                        :execution-id     eid
                                                                        :state-machine-id state-machine-id
                                                                        :original-input   input
                                                                        :resumers         {rid {:then (select-keys invoke [:state :context])}}}
                                               :execution/state        next-state
                                               :execution/memory       next-data})
                                       [{:op           :execution/start
                                         :args         {:state-machine-id state-machine-id
                                                        :execution-id     eid
                                                        :input            (eval-expr! statem-input data input)
                                                        :async?           async?}
                                         :complete-ref rid}]
                                       [{:id (:id action)}]
                                       nil))

                           (:execution invoke)
                           (let [async?     (boolean (:async? invoke))
                                 aid        (:id action)
                                 rid        (random-resume-id!)
                                 eid        (random-execution-id!)
                                 exec-input (:input invoke)
                                 next-state (or (:state invoke) state)
                                 next-data  (if (:context action)
                                              (eval-expr! (:context action) data input)
                                              data)]
                             (->Result (merge (next-ver execution)
                                              {:execution/pause-state  "wait"
                                               :execution/pause-memory {:action-id      aid
                                                                        :execution-id   eid
                                                                        :original-input input
                                                                        :resumers       {rid {:then (select-keys invoke [:state :context])}}}
                                               :execution/state        next-state
                                               :execution/memory       next-data})
                                       [{:op           :execution/step
                                         :args         {:execution-id eid
                                                        :action       action-name
                                                        :input        (eval-expr! exec-input data input)
                                                        :async?       async?}
                                         :complete-ref rid}]
                                       [{:id (:id action)}]
                                       nil))

                           (:call invoke)
                           (let [effect (:call invoke)
                                 rid    (random-resume-id!)]
                             (->Result (merge (next-ver execution)
                                              {:execution/pause-state  "wait"
                                               :execution/pause-memory {:action-id      (:id action)
                                                                        :original-input input
                                                                        :resumers       {rid {:then (select-keys invoke [:state :context])}}}})
                                       [{:op           :invoke/io
                                         :args         {:input input
                                                        :expr  effect}
                                         :complete-ref rid}]
                                       [{:id (:id action)}]
                                       nil))

                           :else (throw (ex-info "Unrecognized :invoke" {:action action
                                                                         :invoke invoke}))))

                       (:wait-for action)
                       (try?
                        execution action
                        (let [{:keys [seconds timestamp] :as wait-for} (:wait-for action)
                              resume-id                                (random-resume-id!)
                              next-state                               (or (:state action) state)
                              next-data                                (if (:context action)
                                                                         (eval-expr! (:context action) data input)
                                                                         data)]
                          (cond
                            seconds
                            (->Result (merge (next-ver execution)
                                             {:execution/pause-state  "wait"
                                              :execution/pause-memory {:action-id      (:id action)
                                                                       :original-input input
                                                                       :resumers       {resume-id {:then (select-keys wait-for [:state :context])}}}
                                              :execution/state        next-state
                                              :execution/memory       next-data
                                              :execution/input        input})
                                      [{:op           :sleep/seconds
                                        :args         {:seconds (eval-expr! seconds data input)}
                                        :complete-ref resume-id}]
                                      [{:id (:id action)}]
                                      nil)

                            timestamp
                            (->Result (merge (next-ver execution)
                                             {:execution/pause-state  "wait"
                                              :execution/pause-memory {:action-id      (:id action)
                                                                       :original-input input
                                                                       :resumers       {resume-id {:then (select-keys wait-for [:state :context])}}}
                                              :execution/state        next-state
                                              :execution/memory       next-data
                                              :execution/input        input})
                                      [{:op           :sleep/timestamp
                                        :args         {:timestamp (eval-expr! timestamp data input)}
                                        :complete-ref resume-id}]
                                      [{:id (:id action)}]
                                      nil)

                            :else
                            (->Result execution
                                      nil
                                      [{:id (:id action)}]
                                      #:error{:code   :invalid-action
                                              :reason "Unrecognized wait-for action"
                                              :action action}))))

                       (:if action)
                       (try?
                        execution action
                        (let [condition  (eval-expr! (:if action) data input)
                              clause     (if condition (:then condition) (:else condition))
                              next-state (or (:state clause) state)
                              next-data  (if (:context clause)
                                           (eval-expr! (:context clause) data input)
                                           data)]
                          (->Result (merge (next-ver execution)
                                           {:execution/pause-state  (ready-or-await-pause-state states next-state)
                                            :execution/pause-memory nil
                                            :execution/state        next-state
                                            :execution/memory       next-data
                                            :execution/input        input})
                                    nil
                                    [{:id (:id action)}]
                                    nil)))

                       (or (:state action) (:context action))
                       (let [next-state (or (:state action) state)
                             next-data  (if (:context action)
                                          (eval-expr! (:context action) data input)
                                          data)]
                         (->Result (merge (next-ver execution)
                                          {:execution/pause-state  (ready-or-await-pause-state states next-state)
                                           :execution/pause-memory nil
                                           :execution/state        next-state
                                           :execution/memory       next-data
                                           :execution/input        input})
                                   nil
                                   [{:id (:id action)}]
                                   nil))

                       :else
                       (->Result execution
                                 nil
                                 nil
                                 #:error{:code    :invalid-action
                                         :reason  "Unrecognized action"
                                         :choices actions
                                         :action  action}))
                     (recur (inc i)))))))))
       assoc :previous execution))))


(comment
  (do
    (def order-statem
      #:state-machine{:id             "order"
                      :version        1
                      :start-at       "create"
                      :execution-mode "async-throughput"
                      :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                      :states         '{"create"    {:actions [{:id    "created"
                                                                :state "cart"}]}
                                        "cart"      {:actions [{:id      "add"
                                                                :when    "add"
                                                                :state   "cart"
                                                                :context (update-in ctx [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
                                                               {:id      "remove"
                                                                :when    "remove"
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
                                                                           (update-in ctx [:order :line-items] (fnil sub []) (repeat (:qty input 1) (:sku input))))}
                                                               {:id    "submit"
                                                                :when  "submit"
                                                                :state "submitted"}]}
                                        "submitted" {:actions [{:id    "fraud-approve"
                                                                :when  "fraud-approve"
                                                                :state "fraud-approved"}
                                                               {:id    "fraud-reject"
                                                                :when  "fraud-reject"
                                                                :state "fraud-rejected"}]}

                                        "fraud-approved"     {:actions [{:id    "release-for-shipping"
                                                                         :state "to-release"}]}
                                        "fraud-rejected"     {:actions [{:id "cancel" :state "canceled"}]}
                                        "to-release"         {:actions [{:id     "ship"
                                                                         :invoke {:state-machine ["shipment" 1]
                                                                                  :input         {:order (:id (:order ctx))}
                                                                                  :state         "submitted-shipment"
                                                                                  :context       (assoc ctx :delivered (:delivered (:execution/return-data output)))}}]}
                                        "submitted-shipment" {:actions [{:id    "shipped"
                                                                         :state "shipped"
                                                                         :when  (:delivered ctx)}
                                                                        {:id    "cancel"
                                                                         :state "canceled"}]}
                                        "shipped"            {:return true}
                                        "canceled"           {:return false}}})

    (def e1 (empty-execution {:sync?           false
                              :execution-id    #uuid "A0A5CD52-7866-4187-B547-F218DDA33B6B"
                              :state-machine   order-statem
                              :initial-context {:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                              :now             (System/nanoTime)}))

    (require '[net.jeffhui.workflow.interpreters :as interpreters :refer [->Sandboxed ->Naive]])

    (def cofx
      {:eval-expr!           (fn
                               ([expr context input] (interpreters/eval-action expr #(throw (ex-info "no io")) context input :net.jeffhui.workflow.protocol/nothing))
                               ([expr context input output] (interpreters/eval-action expr #(throw (ex-info "no io")) context input output)))
       :current-time!        #(System/nanoTime)
       :random-resume-id!    #(vector :resume/id (UUID/randomUUID))
       :random-execution-id! #(vector :execution/id (UUID/randomUUID))}))

  (do
    (def e2 (next-execution cofx order-statem e1 nil))
    (def e3 (next-execution cofx order-statem (:execution e2) {::action "add"
                                                               :qty     1
                                                               :sku     "A1"}))
    (def e4 (next-execution cofx order-statem (:execution e3) {::action "submit"}))
    (def e5 (next-execution cofx order-statem (:execution e4) {::action "fraud-approve"}))
    (def e6 (next-execution cofx order-statem (:execution e5) nil))
    (def e7 (next-execution cofx order-statem (:execution e6) nil)))

  e2
  e3
  e4
  e5
  e6
  e7


  (time
   (dotimes [i 1000]
     (do
       (def se2 (step-execution cofx order-statem e1 nil))
       (def se3 (step-execution cofx order-statem (:execution se2) {::action "add"
                                                                    :qty     1
                                                                    :sku     "A1"}))
       (def se4 (step-execution cofx order-statem (:execution se3) {::action "submit"}))
       (def se5 (step-execution cofx order-statem (:execution se4) {::action "fraud-approve"}))
       (def se6 (step-execution cofx order-statem (:execution se5) {::resume {:id (:complete-ref (first (:effects se5)))
                                                                              :return {:execution/return-data {:delivered true}}}}))
       (def se7 (step-execution cofx order-statem (:execution se6) nil))
       (def se8 (step-execution cofx order-statem (:execution se7) {::action "TEST"})))))

  se2
  se3
  se4
  se5
  se6
  se7
  se8

  (meta (first (:transitions se6)))

  (next-execution cofx order-statem (:execution se5)
                  {::resume {:id (:complete-ref (first (:effects se5)))
                             :return {:execution/return-data {:delivered true}}}})

  [se5 se6 se7]

  )
