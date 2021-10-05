(ns workflow.memory
  (:require [workflow.api :as wf]
            [workflow.protocol :as p]
            [clojure.core.async :as async])
  (:import java.time.Instant
           java.time.Duration
           java.util.Date
           java.util.UUID))

(defn- assoc-in-if
  ([m keypath value] (assoc-in-if m keypath (complement boolean) value))
  ([m keypath pred value]
   (if (pred (get-in m keypath))
     (assoc-in m keypath value)
     m)))

(defrecord StateMachinePersistence [state]
  p/StateMachinePersistence
  (fetch-statem [_ state-machine-id version]
    (let [s       @state
          version (if (= version :latest)
                    (apply max 1 (keys (get s state-machine-id)))
                    version)]
      (get-in s [state-machine-id version])))
  (save-statem [_ state-machine]
               (let [{:state-machine/keys [id version]} state-machine]
                 (future (if (and id version (integer? version))
                           {:ok     true
                            :entity (get-in (swap! state assoc-in-if
                                                   [(:state-machine/id state-machine)
                                                    (:state-machine/version state-machine)]
                                                   state-machine)
                                            [(:state-machine/id state-machine) (:state-machine/version state-machine)])}
                           {:ok false})))))

(defn make-statem-persistence []
  (->StateMachinePersistence (atom {})))

(defrecord ExecutionPersistence [state statem-persistence]
  p/ExecutionPersistence
  (executions-for-statem [_ state-machine-id {:keys [version limit offset]}]
    (let [s       @state
          version (if (= :latest version)
                    (:state-machine/version (p/fetch-statem statem-persistence state-machine-id version))
                    version)]
      ;; TODO(jeff): we should filter by state-machine-id, but it's so much easier to dump all for debugging
      (cond->>
       (reverse (sort-by
                 :execution/started-at
                 (cond
                   (= -1 version) nil
                   (= :all version) (mapcat vals (vals s))
                   (integer? version) (filter (comp #{version} :execution/state-machine-version)
                                              (mapcat vals (vals s))))))
        offset (drop offset)
        limit (take limit))))
  (fetch-execution [_ execution-id version]
    (let [s       @state
          version (if (= version :latest)
                    (apply max 1 (keys (get-in s [execution-id])))
                    version)]
      (get-in s [execution-id version])))
  (fetch-execution-history [_ execution-id]
    (let [s     @state
          execs (vals (get-in s [execution-id]))]
      (sort-by execs :version)))
  (save-execution [_ execution]
    (assert (:execution/id execution))
    (assert (:execution/version execution))
    (future {:ok     true
             :entity (get-in (swap! state assoc-in [(:execution/id execution)
                                                    (:execution/version execution)]
                                    execution)
                             [(:execution/id execution) (:execution/version execution)])})))

(defn make-execution-persistence [statem-persistence]
  (->ExecutionPersistence (atom {}) statem-persistence))

(defrecord SchedulerPersistence [state]
  p/SchedulerPersistence
  (save-task [_ timestamp execution-id options]
    (let [task-id (UUID/randomUUID)]
      (swap! state assoc task-id {:task/id                task-id
                                  :task/execution-id      execution-id
                                  :task/execution-options options
                                  :task/start-after       timestamp})
      task-id))
  (runnable-tasks [_ now]
    (let [s @state]
      (swap! state (fn [s] (into {}
                                 (filter (fn [[_ v]] (if-let [after (:task/start-after v)]
                                                       (not (> (.getSeconds (Duration/between (Instant/now) (.toInstant ^Date after))) 60))
                                                       true)))
                                 s)))
      (keep #(let [after (:task/start-after %)]
               (when (and after (.isAfter (.toInstant ^Date after)
                                          (.toInstant ^Date now)))
                 (select-keys % [:task/id :task/execution-id :task/execution-options :task/response])))
            (vals s))))
  (complete-task [_ task-id reply]
    (swap! state (fn [s r] (if (contains? (s task-id) :task/response)
                             (assoc-in s [task-id :task/response] r)
                             s))
           reply)
    true))

(defn make-scheduler-persistence []
  (->SchedulerPersistence (atom {})))

(defrecord Scheduler [work-ch handler]
  p/Scheduler
  (sleep-to [_ timestamp execution-id options]
    (async/go (async/<! (async/timeout (.toMillis (Duration/between (Instant/now) (.toInstant timestamp)))))
              (async/>! work-ch [execution-id options nil]))
    true)
  (enqueue-execution [_ execution options]
    (let [reply (when (::wf/reply? options) (async/chan 1))]
      (assert (:execution/id execution))
      (assert (:execution/version execution))
      (async/put! work-ch [(:execution/id execution) options reply])
      reply))
  (register-execution-handler [_ f]
    (when (reset! handler f)
      (async/thread
        (loop []
          (when-let [f @handler]
            (let [[eid options reply] (async/<!! work-ch)]
              (when eid
                #_(prn "HANDLE" eid options (Thread/currentThread))
                (let [res (f [[eid options]])]
                  (when reply
                    (async/>!! reply res)
                    (async/close! reply)))
                (recur))))))))
  java.io.Closeable
  (close [_] (reset! handler nil)))

(defn make-scheduler
  ([] (make-scheduler 64))
  ([buf-size] (->Scheduler (async/chan buf-size) (atom {}))))

(comment
  (do
    (do
      (require '[workflow.interpreters :refer [->Sandboxed ->Naive]])
      (defn make
        ([] (let [statem (make-statem-persistence)]
              (wf/effects {:statem      statem
                           :execution   (make-execution-persistence statem)
                           :scheduler   (make-scheduler)
                           :interpreter (->Sandboxed)})))
        ([buf-size] (let [statem (make-statem-persistence)]
                      (wf/effects {:statem      statem
                                   :execution   (make-execution-persistence statem)
                                   :scheduler   (make-scheduler buf-size)
                                   :interpreter (->Sandboxed)}))))
      (def fx (make))
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
      (p/register-execution-handler fx (partial wf/run-executions fx :example))
      (def out (wf/start fx "order" nil)))
    (do
      (wf/trigger fx (second out) {::wf/action "add"
                                   ::wf/reply? true
                                   :sku       "bns12"
                                   :qty       1})
      #_(Thread/sleep 100)
      (wf/trigger fx (second out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                            ::wf/reply? true}))
      (async/take! res prn)))


  (do
    (do
      (require '[workflow.interpreters :refer [->Sandboxed ->Naive]])
      (defn make 
        ([] (let [statem (make-statem-persistence)]
              (wf/effects {:statem      statem
                           :execution   (make-execution-persistence statem)
                           :scheduler   (make-scheduler)
                           :interpreter (->Sandboxed)})))
        ([buf-size] (let [statem (make-statem-persistence)]
                      (wf/effects {:statem      statem
                                   :execution   (make-execution-persistence statem)
                                   :scheduler   (make-scheduler buf-size)
                                   :interpreter (->Sandboxed)}))))
      (def fx (make))
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
                                                                                              (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}}):if
                                                                                              (<= 200 (:status output) 299):then
                                                                                              {:state   "fetched"
                                                                                               :context {:response {:n (:n (:json (:body output)))}}}:else
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
      (p/register-execution-handler fx (partial wf/run-executions fx :example))
      (def out (wf/start fx "order" nil)))
    (do
      (wf/trigger fx (second out) {::wf/action "add"
                                   ::wf/reply? true
                                   :sku       "bns12"
                                   :qty       1})
      #_(Thread/sleep 100)
      (wf/trigger fx (second out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                            ::wf/reply? true}))
      (async/take! res prn))
    )


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
                                :execution/memory])))
         (wf/executions-for-statem fx "order" {:version :latest}))))

  (clojure.pprint/print-table
   (sort-by (juxt :execution/step-started-at :execution/state-machine-id :execution/version)
            (wf/executions-for-statem fx "order" {:version :latest})))

  (doseq [[_ execs] (group-by
                     :execution/id
                     (sort-by (juxt :execution/state-machine-id :execution/version)
                              (wf/executions-for-statem fx "order" {:version :latest})))
          :let [e (last execs)]]
    (println (:execution/state-machine-id e) "took" (double (/ (- (:execution/finished-at e) (:execution/enqueued-at e))
                                                               1000000))
             "ms"))
  )
