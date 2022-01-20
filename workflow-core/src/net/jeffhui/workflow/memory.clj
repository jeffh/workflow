(ns net.jeffhui.workflow.memory
  (:require [net.jeffhui.workflow.api :as wf]
            [net.jeffhui.workflow.protocol :as p]
            [clojure.core.async :as async]
            [net.jeffhui.workflow.contracts :as contracts]
            [net.jeffhui.workflow.api :as api])
  (:import java.time.Instant
           java.time.Duration
           java.util.Date
           java.util.UUID))

(defn- assoc-in-if [m keypath pred value]
  (if (pred (get-in m keypath))
    (assoc-in m keypath value)
    m))

(defrecord StateMachinePersistence [state]
  p/StateMachinePersistence
  ;; TODO: needs contract
  (list-statem [_ {:keys [version limit offset]}]
    (let [s @state
          machines (mapcat vals s)]
      (cond->> machines
        (= :latest version) (filter (comp (into #{} (mapcat (juxt first (comp  vals))) s)
                                          (juxt :state-machine/id :state-machine/version)))
        offset (drop offset)
        limit (take limit))))
  (fetch-statem [_ state-machine-id version]
    (let [s       @state
          version (if (= version :latest)
                    (apply max 1 (keys (get s state-machine-id)))
                    version)]
      (get-in s [state-machine-id version])))
  (save-statem [_ state-machine options]
    (let [{:state-machine/keys [id version]} state-machine]
      (future (if (and id version (integer? version))
                {:ok     true
                 :value (get-in (swap! state assoc-in-if
                                       [(:state-machine/id state-machine)
                                        (:state-machine/version state-machine)]
                                       nil?
                                       state-machine)
                                [(:state-machine/id state-machine) (:state-machine/version state-machine)])}
                {:ok false})))))

(defn make-statem-persistence []
  (->StateMachinePersistence (atom {})))

(defrecord ExecutionPersistence [state]
  p/ExecutionPersistence
  ;; TODO: needs contract
  (list-executions [_ {:keys [limit offset]}]
    (let [s @state]
      (->>
       (mapcat vals (vals s))
       (sort-by :execution/enqueued-at)
       reverse
       (drop (or offset 0))
       (take (or limit 100)))))
  (executions-for-statem [_ state-machine-id {:keys [version limit offset reverse?]}]
    (let [s @state]
      (cond->> (->>
                (group-by :execution/id
                          (cond
                            (nil? version) nil
                            (= :all version) (mapcat vals (vals s))
                            (integer? version) (filter (comp #{version} :execution/state-machine-version)
                                                       (mapcat vals (vals s)))))
                (vals)
                (map last)
                (filter (comp #{state-machine-id} :execution/state-machine-id))
                (sort-by (juxt :execution/enqueued-at)))
        reverse? reverse
        offset   (drop offset)
        limit    (take limit))))
  (fetch-execution [_ execution-id version]
    (let [s       @state
          version (if (= version :latest)
                    (apply max 1 (keys (get-in s [execution-id])))
                    version)]
      (get-in s [execution-id version])))
  (fetch-execution-history [_ execution-id]
    (let [s     @state
          execs (vals (get-in s [execution-id]))]
      (sort-by :execution/version execs)))
  (save-execution [_ execution options]
    (assert (:execution/id execution))
    (assert (:execution/version execution))
    (future {:ok    true
             :value (get-in (swap! state assoc-in [(:execution/id execution)
                                                   (:execution/version execution)]
                                   execution)
                            [(:execution/id execution) (:execution/version execution)])})))

(defn make-execution-persistence []
  (->ExecutionPersistence (atom {})))

(defn- save-task* [state task]
  (assert (:task/id task))
  (swap! state assoc (:task/id task) task)
  {:error nil})

(defn- runnable-tasks* [state now]
  (let [s @state]
    (swap! state (fn [s] (into {} (remove (fn [[_ v]] (:task/complete? v))) s)))
    (filter #(let [after (:task/start-after %)]
               (.isAfter (.toInstant ^Date now)
                         (.toInstant ^Date after)))
            (vals s))))

(defn- complete-task* [state task-id reply]
  (swap! state (fn [s r] (if (contains? (s task-id) :task/response)
                           (update s task-id merge {:task/response  r
                                                    :task/complete? true})
                           s))
         reply)
  true)

(defn- delete-task* [state task-id]
  (swap! state dissoc task-id)
  true)

(defrecord SchedulerPersistence [state]
  p/SchedulerPersistence
  (save-task [_ task] (future (save-task* state task)))
  (runnable-tasks [_ now] (runnable-tasks* state now))
  (complete-task [_ task-id reply] (future (complete-task* state task-id reply)))
  (delete-task [_ task-id] (future (delete-task* state task-id))))

(defn make-scheduler-persistence []
  (->SchedulerPersistence (atom {})))

(defrecord Scheduler [work-ch handler]
  p/Scheduler
  (sleep-to [_ timestamp execution-id options]
    (let [reply (when (::wf/reply? options) (async/chan 1))]
      (async/go (async/<! (async/timeout (inc (.toMillis (Duration/between (Instant/now) (.toInstant timestamp))))))
                (async/>! work-ch [execution-id (dissoc options ::wf/reply?) reply]))
      reply))
  (enqueue-execution [_ execution-id options]
    (assert (uuid? execution-id))
    (let [reply (when (::wf/reply? options) (async/chan 1))]
      (async/put! work-ch [execution-id options reply])
      reply))
  (register-execution-handler [_ f]
    (when (reset! handler f)
      (async/thread
        (loop []
          (when-let [f @handler]
            (let [[eid options reply] (async/<!! work-ch)]
              (when eid
                (let [res (f eid options)]
                  (when reply
                    (when res
                      (async/>!! reply res))
                    (async/close! reply)))
                (recur))))))))
  p/Connection
  (open* [this]
    (assoc this
           :work-ch (or work-ch (async/chan 64))
           :handler (atom {})))
  (close* [this]
    (reset! handler nil)
    (async/close! work-ch)
    (assoc this :work-ch nil)))

(defn make-scheduler
  ([] (make-scheduler 64))
  ([buf-size] (->Scheduler (async/chan buf-size) (atom {}))))

(comment
  (def store (atom []))
  (def tapper (fn [m] (swap! store conj m)))
  (add-tap tapper)
  (remove-tap tapper)
  (net.jeffhui.workflow.tracer/reset-taps)

  (do
    (do
      (require '[net.jeffhui.workflow.interpreters :refer [->Sandboxed ->Naive]] :reload)
      (require '[net.jeffhui.workflow.contracts :as contracts])
      (defn make
        ([] (wf/effects {:statem      (make-statem-persistence)
                         :execution   (make-execution-persistence)
                         :scheduler   (make-scheduler)
                         :interpreter (->Sandboxed)}))
        ([buf-size] (wf/effects {:statem      (make-statem-persistence)
                                 :execution   (make-execution-persistence)
                                 :scheduler   (make-scheduler buf-size)
                                 :interpreter (->Sandboxed)})))
      (def fx (api/open (make)))
      (wf/save-statem fx contracts/prepare-cart-statem)
      (wf/save-statem fx contracts/order-statem)
      (wf/save-statem fx contracts/shipment-statem)
      (p/register-execution-handler fx (wf/create-execution-handler fx)))

    #_
    (do
      (def out (wf/start fx "prepare-cart" {:skus #{"A1" "B2"}})))

    (do
      (def out (wf/start fx "order" {::wf/io {"http.request.json" (fn [method uri res]
                                                                    {:status 200
                                                                     :body   (:json-body res)})}}))

      (wf/trigger fx (:execution/id out) {::wf/action "add"
                                          ::wf/reply? true
                                          :sku        "bns12"
                                          :qty        1})

      #_(Thread/sleep 100)
      (wf/trigger fx (:execution/id out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (def res (wf/trigger fx (:execution/id out) {::wf/action "fraud-approve"
                                                   ::wf/reply? true}))
      (async/take! res prn)))

  (count @store)

  out

  (.pid (java.lang.ProcessHandle/current))

  (clojure.pprint/print-table
   (sort-by
    (juxt :t)
    (map (fn [sm]
           (merge {:execution/event-name ""}
                  (select-keys (assoc sm :t [(or (:execution/step-started-at sm) (:execution/enqueued-at sm))
                                             (:execution/version sm)])
                               [:execution/state-machine-id
                                :execution/state
                                :execution/pause-state
                                :execution/pending-effects
                                :execution/completed-effects
                                :execution/comment
                                :execution/input
                                :t
                                #_:execution/error
                                :execution/ctx])))
         (take 100
               (mapcat #(wf/fetch-execution-history fx (:execution/id %))
                       (wf/executions-for-statem fx "order" {:version :latest}))))))

  (take 2
        (take 100
              (mapcat #(wf/fetch-execution-history fx (:execution/id %))
                      (wf/executions-for-statem fx "order" {:version :latest}))))

  (wf/fetch-execution fx (:execution/id out) :latest)
  (clojure.pprint/print-table
   (map #(select-keys % [:execution/state
                         :execution/state-machine-id
                         :execution/pause-state
                         :execution/comment
                         :execution/input
                         :execution/pending-effects
                         :execution/completed-effects])
        (wf/fetch-execution-history fx (:execution/id out))))

  (map (partial wf/fetch-execution-history fx)
       (distinct (keep (comp first wf/child-execution-ids) (wf/fetch-execution-history fx (:execution/id out)))))

  (do
    (require 'clojure.inspector)
    (clojure.inspector/inspect-tree
     (sort-by (juxt :execution/state-machine-id :execution/version)
              (take 100
                    (mapcat #(wf/fetch-execution-history fx (:execution/id %))
                            (wf/executions-for-statem fx "order" {:version :latest}))))))

  (def res
    (take 10
          (sort-by :execution/version
                   (take 100
                         (filter (fn [e] (< (:execution/version e) 100))
                                 (filter (comp #{"shipment"} :execution/state-machine-id)
                                         (mapcat vals (vals @(:state (:execution-persistence fx))))))))))

  (map (comp (juxt identity #(map meta %)) :execution/debug) res)

  (clojure.pprint/print-table
   (take 10
         (sort-by :execution/version
                  (vals (get @(:state (:execution-persistence fx)) (:execution/id out))))))

  (clojure.pprint/print-table
   (sort-by (juxt :execution/step-started-at :execution/state-machine-id :execution/version)
            (wf/executions-for-statem fx "order" {:version :latest})))

  (doseq [[_ execs] (group-by
                     :execution/id
                     (sort-by (juxt :execution/state-machine-id :execution/version)
                              (wf/executions-for-statem fx "order" {:version :latest})))
          :let      [e (last execs)]]
    (println (:execution/state-machine-id e) "took" (double (/ (- (:execution/finished-at e) (:execution/enqueued-at e))
                                                               1000000))
             "ms"))
  )
