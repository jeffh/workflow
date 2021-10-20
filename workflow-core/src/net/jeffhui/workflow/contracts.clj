(ns net.jeffhui.workflow.contracts
  (:require [clojure.test :refer [testing is]]
            [clojure.core.async :as async]
            [net.jeffhui.workflow.api :as api]
            [net.jeffhui.workflow.protocol :as protocol]
            [net.jeffhui.workflow.interpreters :refer [->Sandboxed]]))

(declare order-statem shipment-statem)
(defn statem-persistence [doc-name creator]
  (testing doc-name
    (testing "conforms to state machine persistence"
      (let [persistence (api/open (creator))]
        (try
          (testing "[happy path]"
            (testing "save-statem returns a future that saves the state machine"
              (let [r (api/save-statem persistence order-statem)]
                (is (future? r) "save-statem should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-statem should succeed")
                (is (= order-statem (:entity (deref r 1000 :timeout))))))

            (testing "fetch-statem returns previously stored state machines"
              (is (= order-statem (api/fetch-statem persistence
                                                    (:state-machine/id order-statem)
                                                    (:state-machine/version order-statem)))
                  "persistence should return what was stored")
              (is (= order-statem (api/fetch-statem persistence (:state-machine/id order-statem) :latest))
                  "persistence should return latest state machine version"))

            (testing "fetching :latest returns the latest state machine version"
              (let [v2 (assoc order-statem :state-machine/version 2)]
                (is (:ok (deref (api/save-statem persistence v2)
                                1000 :timeout))
                    "given version 2 of a state machine is saved")
                (is (= v2 (api/fetch-statem persistence (:state-machine/id order-statem) :latest))
                    "using version = :latest should return v2")))

            (testing "checking that v1 of statem still exists after saving v2"
              (is (= order-statem (api/fetch-statem persistence
                                                    (:state-machine/id order-statem)
                                                    (:state-machine/version order-statem)))
                  "persistence should return what was stored")))

          (testing "[exceptional cases]"
            (testing "ignores duplicate state machine version"
              (let [r (api/save-statem persistence (assoc shipment-statem
                                                          :state-machine/id (:state-machine/id order-statem)
                                                          :state-machine/version (:state-machine/version order-statem)))]
                (is (not= :timeout (deref r 1000 :timeout))
                    "save may succeed or fail"))

              (is (= order-statem (api/fetch-statem persistence (:state-machine/id order-statem) (:state-machine/version order-statem)))
                  "the original state machine remains unchanged")))

          (testing "[input validation cases]"
            (testing "persistence ignores saves without an id and version"
              (letfn [(error-save [statem]
                        (not (:ok (deref (api/save-statem persistence statem) 1000 :timeout))))]
                (is (error-save (dissoc order-statem :state-machine/id))
                    "errors without state machine id")
                (is (error-save (dissoc order-statem :state-machine/version))
                    "errors without state machine version")
                (is (error-save (dissoc order-statem :state-machine/id :state-machine/version))
                    "errors without state machine id and version")
                (testing "errors when :state-machine/version is not an integer"
                  (is (error-save (assoc order-statem :state-machine/version "1")))
                  (is (error-save (assoc order-statem :state-machine/version 1.2)))
                  (is (error-save (assoc order-statem :state-machine/version 1M)))))))

          (finally
            (api/close persistence)))))))


(declare execution-started execution-step execution2-started)
(defn execution-persistence [doc-name creator]
  (testing doc-name
    (testing "conforms to execution persistence"
      (let [persistence (api/open (creator))]
        (try
          (testing "[happy path - 1 execution; 1 step]"
            (testing "save-execution returns a future that saves the execution"
              (let [r (protocol/save-execution persistence execution-started)]
                (is (future? r) "save-execution should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-execution should succeed")
                (is (= execution-started (:entity (deref r 1000 :timeout))))))
            (testing "fetch-execution returns the execution of the specific version"
              (is (= execution-started (api/fetch-execution persistence
                                                            (:execution/id execution-started)
                                                            (:execution/version execution-started)))
                  "persistence should return what was stored"))
            (testing "fetch-execution-history returns a seq of executions"
              (is (= (seq [execution-started]) (api/fetch-execution-history persistence (:execution/id execution-started)))
                  "persistence should return a single execution-started"))
            (testing "executions-for-statem returns a seq of executions for a given statem"
              (is (= (seq [execution-started]) (api/executions-for-statem persistence
                                                                          (:execution/state-machine-id execution-started)
                                                                          {:version (:execution/state-machine-version execution-started)}))
                  "persistence should return a single execution when fetching by state machine")))
          (testing "[happy path - 1 execution; 2 steps]"
            (testing "save-execution returns a future that saves the execution"
              (let [r (protocol/save-execution persistence execution-step)]
                (is (future? r) "save-execution should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-execution should succeed")
                (is (= execution-step (:entity (deref r 1000 :timeout))))))
            (testing "fetch-execution returns the execution of the specific version"
              (is (= execution-step (api/fetch-execution persistence
                                                            (:execution/id execution-step)
                                                            (:execution/version execution-step)))
                  "persistence should return what was stored"))
            (testing "fetch-execution-history returns a history seq of executions, ordered by latest last"
              (is (= (seq [execution-started execution-step]) (api/fetch-execution-history persistence (:execution/id execution-step)))
                  "persistence should return a single execution-started"))
            (testing "executions-for-statem returns a seq of executions for a given statem, ordered by latest started-at"
              (is (= (seq [execution-step]) (api/executions-for-statem persistence
                                                                       (:execution/state-machine-id execution-step)
                                                                       {:version (:execution/state-machine-version execution-step)}))
                  "persistence should return a single execution when fetching by state machine")))

          (testing "[happy path - 2 executions; 2 steps, 1 step]"
            (testing "save-execution returns a future that saves the execution"
              (let [r (protocol/save-execution persistence execution2-started)]
                (is (future? r) "save-execution should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-execution should succeed")
                (is (= execution2-started (:entity (deref r 1000 :timeout))))))
            (testing "fetch-execution returns the execution of the specific version"
              (is (= execution2-started (api/fetch-execution persistence
                                                             (:execution/id execution2-started)
                                                             (:execution/version execution2-started)))
                  "persistence should return what was stored"))
            (testing "fetch-execution-history returns a history seq of executions, ordered by latest last"
              (is (= (seq [execution-started execution-step]) (api/fetch-execution-history persistence (:execution/id execution-started)))
                  "persistence should return a two execution historical values for execution1")
              (is (= (seq [execution2-started]) (api/fetch-execution-history persistence (:execution/id execution2-started)))
                  "persistence should return a one execution historical value for execution2"))
            (testing "executions-for-statem returns a seq of executions for a given statem, ordered by latest started-at"
              (is (= (seq [execution2-started execution-step]) (api/executions-for-statem persistence
                                                                                          (:execution/state-machine-id execution-step)
                                                                                          {:version (:execution/state-machine-version execution-step)}))
                  "persistence should return 2 executions when fetching by state machine")))

          (testing "[exceptional cases]"
            (testing "errors with duplicate execution versions"
              (let [r (protocol/save-execution persistence execution-started)]
                (is (not= :timeout (deref r 1000 :timeout))
                    "save may succeed or fail"))

              (is (= execution-started (api/fetch-execution persistence (:execution/id execution-started) (:execution/version execution-started)))
                  "the original state machine remains unchanged")))

          (finally
            (api/close persistence)))))))

(defn scheduler [doc-name creator]
  (testing doc-name
    (testing "conforms to a scheduler"
      (let [sch     (api/open (creator))
            queue   (async/chan 16)
            replies (async/chan 16)]
        (api/register-execution-handler sch (fn [execution-id execution-options]
                                              (async/>!! queue [execution-id execution-options])
                                              (async/<!! replies)))
        (try
          (testing "[happy path]"
            (testing "enqueuing immediate execution, expecting no response"
              (protocol/enqueue-execution sch #uuid "36179744-6E36-43CB-B378-28A0E380F7C8" {:argument 1})
              (is (= [#uuid "36179744-6E36-43CB-B378-28A0E380F7C8" {:argument 1}] (async/<!! queue)))
              (async/>!! replies {:test 1}))
            (testing "enqueue immediate execution, expecting response"
              (let [res (protocol/enqueue-execution sch #uuid "C4F7B251-B4A9-4156-A810-40CA67CC3788" {:argument 2 ::api/reply? true})]
                (is (= [#uuid "C4F7B251-B4A9-4156-A810-40CA67CC3788" {:argument 2}]
                       (update (async/<!! queue) 1 dissoc ::api/reply?)))
                (async/>!! replies {:test 2})
                (is (= {:test 2} (async/<!! res))
                    "expect to receive reply")))
            (testing "sleep execution later, expecting no response"
              (let [sleep-duration 100
                    res            (protocol/sleep sch sleep-duration #uuid "BB2F1B81-4EEE-443F-A874-D32EF41968F5" {:argument 3})
                    start          (System/nanoTime)
                    _              (is (= [#uuid "BB2F1B81-4EEE-443F-A874-D32EF41968F5" {:argument 3}]
                                          (loop [x (async/<!! queue)]
                                            (if (= #uuid "BB2F1B81-4EEE-443F-A874-D32EF41968F5" (first x))
                                              x
                                              (recur (async/<!! queue))))))
                    end            (System/nanoTime)
                    delta-ms       (double (/ (- end start) 1000000))]
                (is (>= delta-ms sleep-duration)
                    "execution is deferred by at least the given amount")
                (async/>!! replies {:test 3}))))
          (testing "[exceptional cases]"
            (testing "sleeping in the past still runs"
              (let [sleep-duration -1000
                    res            (protocol/sleep sch sleep-duration #uuid "6314F257-CFCD-4A14-A123-EB188E479F8A" {:argument 4})
                    start          (System/nanoTime)
                    _              (is (= [#uuid "6314F257-CFCD-4A14-A123-EB188E479F8A" {:argument 4}]
                                          (loop [x (async/<!! queue)]
                                            (if (= #uuid "6314F257-CFCD-4A14-A123-EB188E479F8A" (first x))
                                              x
                                              (recur (async/<!! queue))))))
                    end            (System/nanoTime)
                    delta-ms       (double (/ (- end start) 1000000))]
                (is (pos? delta-ms)
                    "execution occurs")
                (async/>!! replies {:test 4}))))
          (finally
            (async/close! queue)
            (async/close! replies)
            (api/close sch)))))))

(defmacro letlocals
  "Allows local definition of variables that's useful for side-effectful sequences of code.

  (letlocal
   (bind x 1)
   (bind y 2)
   (identity y)
   (+ x y))

  ;; => (let [x 1 y 2 _ (identity y)] (+ x y))

  NOTE: bind form must be direct child of letlocal
  "
  [& forms]
  (let [tmpvar   (gensym "_")
        bindings (mapcat
                  identity
                  (for [f (butlast forms)]
                    (or (when (seq? f)
                          (let [sym (first f)]
                            (when (= 'bind sym)
                              [(second f) (nth f 2)])))
                        [tmpvar f])))]
    `(let ~(vec bindings)
       ~(last forms))))

(defn- print-executions [fx state-machine-id]
  (clojure.pprint/print-table
   (sort-by
    (juxt :t)
    (map (fn [sm]
           (merge {:execution/event-name ""}
                  (-> sm
                      (assoc :t [(or (:execution/step-started-at sm) (:execution/enqueued-at sm))
                                 (:execution/version sm)])
                      (select-keys
                       [:execution/state-machine-id
                        :execution/state
                        :execution/status
                        :execution/event-name
                        :execution/comment
                        :execution/input
                        :t
                        :execution/error
                        :execution/memory])
                      (cond-> (get-in sm [:execution/input ::api/io]) (assoc-in [:execution/input ::api/io] '...)))))
         (mapcat #(api/fetch-execution-history fx (:execution/id %))
                 (api/executions-for-statem fx state-machine-id {:version :latest}))))))

(declare wishlist-statem prepare-cart-statem)
(defmacro ^:private with-each-fx [[fx-sym fx-uninit] setup & unit-tests]
  `(do
     ~@(for [form unit-tests]
         `(let [~fx-sym (api/open ~fx-uninit)]
            (try
              ~setup
              ~form
              (finally
                (api/close ~fx-sym)))))))
(defn effects [doc-name fx-options]
  (testing doc-name
    (testing "is well integrated:"
      (with-each-fx [fx (api/effects (merge
                                      {:interpreter (->Sandboxed)}
                                      (fx-options)))]
        (testing "setting up state machines"
          (doseq [statem [wishlist-statem
                          prepare-cart-statem
                          order-statem
                          shipment-statem]]
            (let [res @(api/save-statem fx statem)]
              (is (:ok res)
                  (format "failed to save statem: %s: %s"
                          (:state-machine/id statem)
                          (pr-str res)))))
          (api/register-execution-handler fx (api/create-execution-handler fx)))

        ;;; test cases
        (testing "running a sample order"
          (letlocals
           (bind [ok execution-id _finished-execution] (api/start fx "order" {::api/io '{"http.request.json" (fn [method uri res]
                                                                                                               {:status 200
                                                                                                                :body   {:json {:n (get (:json-body res) "n")}}})}}))
           (is ok "Failed to start 'order' execution")
           (bind res-ch (api/trigger fx execution-id {::api/action "add"
                                                      ::api/reply? true
                                                      :sku         "bns12"
                                                      :qty         1}))
           (is res-ch "expected reply channel")
           (is (not (nil? (async/<!! res-ch)))
               "failed to add to order")
           (bind res-ch (api/trigger fx execution-id {::api/action "place"}))
           (is (nil? res-ch))
           (bind res-ch (api/trigger fx execution-id {::api/action "fraud-approve"
                                                      ::api/reply? true}))
           (is res-ch)
           (is (not (nil? (async/<!! res-ch))))
           (testing "verify execution completes"
             (letlocals
              (bind final-execution (loop [attempts 0
                                           e        (api/fetch-execution fx execution-id :latest)]
                                      (if (or (#{"failed" "failed-resumable" "finished"} (:execution/status e))
                                              (< 10 attempts))
                                        e
                                        (do
                                          (Thread/sleep 5000)
                                          (recur (inc attempts) (api/fetch-execution fx execution-id :latest))))))
              (when-not (= "finished" (:execution/status final-execution))
                (print-executions fx "order"))
              (is (= "finished" (:execution/status final-execution))
                  (format "expected execution to complete successfully:\n%s"
                          (pr-str final-execution)))
              #_(print-executions fx "order")))))

        (testing "state machines that trigger other machines asynchronously"
          (letlocals
           (bind [ok execution-id _] (api/start fx "prepare-cart" {:skus #{"A1" "B2"}}))
           (is ok "Failed to start 'prepare-cart' execution")

           (testing "verify execution completes"
             (letlocals
              (bind final-execution (loop [attempts 0
                                           e        (api/fetch-execution fx execution-id :latest)]
                                      (if (or (#{"failed" "failed-resumable" "finished"} (:execution/status e))
                                              (< 10 attempts))
                                        e
                                        (do
                                          (Thread/sleep 5000)
                                          (recur (inc attempts) (api/fetch-execution fx execution-id :latest))))))
              (when-not (= "finished" (:execution/status final-execution))
                (print-executions fx "prepare-cart"))
              (is (= "finished" (:execution/status final-execution))
                  (format "expected execution to complete successfully:\n%s"
                          (pr-str final-execution)))
              (print-executions fx "prepare-cart")
              (print-executions fx "order")))))))))

(def ^:private execution-started
  #:execution{:comment               "Enqueued for execution"
              :event-name            nil
              :event-data            nil
              :id                    #uuid "3DB0A06E-7F9C-445A-B327-CE9B4EEC9E53"
              :version               1
              :state-machine-id      "order"
              :state-machine-version 1
              :io                    nil
              :mode                  "async-throughput"
              :status                "queued"
              :state                 "create"
              :memory                {:order {:id "R4523"}}
              :input                 nil
              :enqueued-at           (System/nanoTime)
              :started-at            nil
              :finished-at           nil
              :failed-at             nil
              :step-started-at       nil
              :step-ended-at         nil
              :user-started-at       nil
              :user-ended-at         nil
              :error                 nil
              :return-target         nil
              :wait-for              nil
              :return-data           nil
              :end-state             nil
              :dispatch-result       nil
              :dispatch-by-input     nil})

(def ^:private execution2-started
  (assoc execution-started
         :execution/id #uuid "7545E6ED-7151-4E0B-B60D-1972AE614D97"
         :execution/memory {:order {:id "R5232"}}
         :execution/enqueued-at (System/nanoTime)))

(def ^:private execution-step
  #:execution{:comment               "Resuming execution (create) on :example"
              :event-name            nil
              :event-data            nil
              :id                    #uuid "3DB0A06E-7F9C-445A-B327-CE9B4EEC9E53"
              :version               2
              :state-machine-id      "order"
              :state-machine-version 1
              :io                    nil
              :mode                  "async-throughput"
              :status                "running"
              :state                 "create"
              :memory                {:order {:id "R4523"}}
              :input                 nil
              :enqueued-at           (System/nanoTime)
              :started-at            nil
              :finished-at           nil
              :failed-at             nil
              :step-started-at       (+ 10 (System/nanoTime))
              :step-ended-at         nil
              :user-started-at       nil
              :user-ended-at         nil
              :error                 nil
              :return-target         nil
              :wait-for              nil
              :return-data           nil
              :end-state             nil
              :dispatch-result       nil
              :dispatch-by-input     nil})

(def wishlist-statem
  #:state-machine{:id             "wishlist"
                  :version        1
                  :start-at       "idle"
                  :execution-mode "async-throughput"
                  :context        {:items #{}}
                  :states         '{"idle" {:actions {"add"     {:name    "added"
                                                                 :state   "idle"
                                                                 :context (update ctx :items (fnil conj #{}) (:sku input))}
                                                      "remove"  {:name    "removed"
                                                                 :state   "idle"
                                                                 :context (update ctx :items disj (:sku input))}
                                                      "convert" {:name   "created-order"
                                                                 :state  "idle"
                                                                 :invoke {:state-machine ["prepare-cart" 1]
                                                                          :async?        true
                                                                          :input         {:skus (:items ctx)}
                                                                          :success       {}
                                                                          :error         {}}}}}}})

(def prepare-cart-statem
  #:state-machine{:id             "prepare-cart"
                  :version        1
                  :start-at       "start"
                  :execution-mode "sync"
                  :timeout-msec   5000
                  :context        '{:requirements (set (:skus input))
                                    :left         (set (:skus input))}
                  :states         '{"start"    {:always [{:name   "create-cart"
                                                          :id     "create-cart"
                                                          :invoke {:state-machine ["order" 1]
                                                                   :async?        true
                                                                   :success       {:state   "adding"
                                                                                   :context (assoc ctx :order-eid (:execution/id output))}
                                                                   :error         {:state "failed"}}}]}
                                    "adding"   {:always [{:name   "added"
                                                          :id     "adding"
                                                          :when   (seq (:left ctx))
                                                          :invoke {:trigger [(:order-eid ctx) "add"]
                                                                   :input   {:sku (first (:left ctx))
                                                                             :qty 1}
                                                                   :success {:state   "complete"
                                                                             :context (-> ctx
                                                                                          (update :left disj (first (:left ctx)))
                                                                                          (assoc :last-output output))}
                                                                   :error   {:state "failed"}}}
                                                         {:name  "complete"
                                                          :state "complete"}]}
                                    "complete" {:return {:ok true}}
                                    "failed"   {:return {:ok false}}}})


(def order-statem
  #:state-machine{:id             "order"
                  :version        1
                  :start-at       "create"
                  :execution-mode "async-throughput"
                  :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                  :states         '{"create"    {:actions [{:id  "created"
                                                            :state "cart"}]}
                                    "cart"      {:actions [{:id      "added"
                                                            :when    "add"
                                                            :state   "cart"
                                                            :context (update-in ctx [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
                                                           {:id      "removed"
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
                                                           {:id    "place"
                                                            :state "submitted"
                                                            :when  "place"}]}
                                    "submitted" {:actions [{:id    "fraud-approve"
                                                            :when  "fraud-approve"
                                                            :state "fraud-approved"}
                                                           {:id    "fraud-reject"
                                                            :when  "fraud-reject"
                                                            :state "fraud-rejected"}]}

                                    "fraud-approved" {:actions [{:id    "release"
                                                                 :state "released"}]}
                                    "fraud-rejected" {:actions {"cancel" {:state "canceled"}}}
                                    "released"       {:actions [{:id     "ship"
                                                                 :invoke {:state-machine ["shipment" 1]
                                                                          :input         {:order (:id (:order ctx))}
                                                                          :context       (assoc ctx {:output output})
                                                                          :state         "finished-ship"}}]}
                                    "finished-ship"  {:actions [{:id    "fulfilled"
                                                                :when  (:delivered (:output ctx))
                                                                :state "shipped"}
                                                               {:id    "canceled"
                                                                :state "canceled"}]}
                                    "shipped"        {:return true}
                                    "canceled"       {:return false}}})

(def shipment-statem
  #:state-machine{:id             "shipment"
                  :version        1
                  :start-at       "created"
                  :execution-mode "async-throughput"
                  :context        '{:id        "S1"
                                    :order     (:order input)
                                    :delivered false}
                  :states         '{"created"     {:actions [{:id    "fulfilled"
                                                              :state "outstanding"}]}
                                    "outstanding" {:actions [{:id     "fetch"
                                                              :invoke {:call (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}})

                                                                       :if   (and (:status output) (<= 200 (:status output) 299))
                                                                       :then {:state   "fetched"
                                                                              :context (assoc ctx :response {:status (:status output)
                                                                                                             :n      (:n (:json (:body output)))})}
                                                                       :else {:state "failed"}}}
                                                             {:id    "cancel"
                                                              :when  "cancel"
                                                              :state "canceled"}]}
                                    "failed" {:actions [{:id       "retry"
                                                         :state    "outstanding"
                                                         :wait-for {:seconds 5}}]}

                                    "canceled" {:return {:delivered false}}

                                    "fetched" {:always [{:name    "deliver"
                                                         :state   "delivered"
                                                         :when    (let [res (:response ctx)]
                                                                    (and (:status res)
                                                                         (<= 200 (:status res) 299)
                                                                         (:n res)
                                                                         (> 3 (:n res))))
                                                         :context {:response nil
                                                                   :result   (:n (:response ctx))}}
                                                        {:name     "retry"
                                                         :state    "outstanding"
                                                         :context  {:response nil}
                                                         :wait-for {:seconds 5}}]}

                                    "delivered" {:return {:delivered true}}}})


