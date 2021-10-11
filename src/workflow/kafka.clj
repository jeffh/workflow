(ns workflow.kafka
  (:require [workflow.protocol :as protocol]
            [workflow.impl.kafka.messaging :as messaging]
            [workflow.memory :as mem]
            [workflow.api :as wf]
            [taoensso.nippy :as nippy]
            [clojure.core.async :as async])
  (:import java.util.Date
           java.util.UUID
           java.time.Instant
           java.util.concurrent.Executors
           java.util.concurrent.ExecutorService))

(defn- responses-handler [producer response-topic persistence handler open-tasks msg]
  (let [task (:value msg)]
    (condp = (:task/op task)
      :complete (when-let [ch (get @open-tasks (:task/id task))]
                  (when-let [res (:task/response task)]
                    (async/put! ch res))
                  (async/close! ch)
                  (swap! open-tasks dissoc (:task/id task)))
      :run (when-let [f (first @handler)]
             (let [res     (f (:task/execution-id task) (:task/execution-options task))
                   enc-res (nippy/fast-freeze (assoc task
                                                     :task/op :complete
                                                     :task/response res))]
               (protocol/complete-task persistence (:task/id task) res)
               ;;  (messaging/send! producer execution-topic (str (:task/execution-id task)) enc-res)
               (messaging/send! producer response-topic (str (:task/execution-id task)) enc-res))))))

(defn- execution-handler [producer persistence handler open-tasks ^ExecutorService pool msg]
  ;; limitations:
  ;;   open-tasks should only be called on completion ops - we need a completion message on a topic
  (.submit
   pool
   ^Runnable
   (fn []
     (let [task (:value msg)]
       ;; (println (format "ExecutionHandler(%s)" (pr-str task)))
       (condp = (:task/op task)
         :run (when-let [f (first @handler)]
                (let [res     (f (:task/execution-id task) (:task/execution-options task))
                      enc-res (nippy/fast-freeze (assoc task
                                                        :task/op :complete
                                                        :task/response res))]
                  (protocol/complete-task persistence (:task/id task) res)
                  ;;  (messaging/send! producer execution-topic (str (:task/execution-id task)) enc-res)
                  (when-let [response-topic (:task/reply-topic task)]
                    (messaging/send! producer response-topic (str (:task/execution-id task)) enc-res)))))))))

(defn- message-handler [producer persistence handler open-tasks msg]
  ;; limitations:
  ;;   open-tasks should only be called on completion ops - we need a completion message on a topic
  (let [task (:value msg)]
    (condp = (:task/op task)
      :complete (when-let [ch (get @open-tasks (:task/id task))]
                  ;; (println (format "MessageHandler(%s, %s)" (pr-str task) (pr-str ch)))
                  (when-let [res (:task/response task)] (async/put! ch res))
                  (async/close! ch)
                  (swap! open-tasks dissoc (:task/id task))))))

(defmacro ^:private try? [& body]
  `(try ~@body (catch Throwable t# nil)))

(defrecord Scheduler [producer responses-worker handler-consumer-fn executions-topic responses-topic persistence handler open-tasks close-poller name-hint ^ExecutorService executor create-poll-task]
  protocol/Scheduler
  (sleep-to [this timestamp execution-id options]
    (let [task-id (protocol/save-task persistence timestamp execution-id options)]
      (swap! open-tasks assoc task-id (async/chan 1))
      (nil? task-id)))
  (enqueue-execution [_ execution-id options]
    (let [reply   (when (::wf/reply? options) (async/chan 1))
          options (dissoc options ::wf/reply?)
          task-id (protocol/save-task persistence (Date/from (.plusMillis (Instant/now) -10000)) execution-id options)]
      (when reply (swap! open-tasks assoc task-id reply))
      (messaging/send! producer executions-topic (str execution-id)
                       (nippy/fast-freeze (merge {:task/op                :run
                                                  :task/id                task-id
                                                  :task/execution-id      execution-id
                                                  :task/execution-options options}
                                                 (when reply {:task/reply-topic responses-topic}))))
      reply))
  (register-execution-handler [_ f]
    (if (and (nil? (second @handler)) f)
      (let [c (handler-consumer-fn)]
        (reset! handler [f (messaging/backgrounded-consumer
                            c
                            {:topics          #{executions-topic}
                             :thread-name     name-hint
                             :process-message (partial execution-handler producer persistence handler open-tasks executor)})]))
      (swap! handler update 0 f))
    true)
  protocol/Connection
  (open* [this]
    (when close-poller (try? (close-poller)))
    (assoc this :close-poller (create-poll-task)))
  (close* [this]
    (try? (close-poller))
    (try? (when-let [worker (second @handler)] (messaging/close! worker)))
    (try? (messaging/close! responses-worker))
    (try? (messaging/close! producer))
    (try? (.shutdown executor))
    (assoc this
           :close-poller nil
           :worker nil
           :responses-worker nil
           :producer nil)))

;; TODO(jeff): rename status-poll-interval-ms to something more API-consumer centric - perhaps recovery-poll-interval-ms?
(defn make-scheduler* [sch-persistence producer responses-consumer make-handler-consumer-fn
                       {:keys [executions-topic responses-topic name
                               status-poll-interval-ms exception-handler
                               executor]
                        :or   {executions-topic        "workflow-executions"
                               responses-topic         "workflow-responses"
                               status-poll-interval-ms 5000}}]
  (assert (>= status-poll-interval-ms 100)
          "Low poll intervals are not allows for recovery fetches (since it introduces bugs)")
  (let [executor         (cond
                           (instance? ExecutorService executor) executor
                           (= 1 executor)                       (Executors/newSingleThreadExecutor)
                           (integer? executor)                  (Executors/newFixedThreadPool (int executor))
                           (nil? executor)                      (Executors/newCachedThreadPool)
                           :else                                (throw (IllegalArgumentException. "Invalid executor, should be nil, integer, or an ExecutorService instance")))
        open-tasks       (atom {})
        handler          (atom nil)
        responses-worker (messaging/backgrounded-consumer
                          responses-consumer
                          {:topics          #{responses-topic}
                           :thread-name     name
                           :process-message (partial message-handler producer sch-persistence handler open-tasks)})
        ;; TODO(jeff): each topic needs its own consumer, response-topic-config needs unique consumer group per every listener
        create-poll-task #(protocol/schedule-recurring-local-task!
                           status-poll-interval-ms status-poll-interval-ms
                           (fn []
                             (try
                               (when-let [open-tasks (not-empty (keys @open-tasks))]
                                 (let [outstanding-tasks (filter (comp (set open-tasks) :task/id)
                                                                 (protocol/runnable-tasks sch-persistence (Date.)))]
                                   (doseq [t     outstanding-tasks
                                           :when (not (:task/complete? t))]
                                     (messaging/send! producer
                                                      executions-topic
                                                      (str (:task/execution-id t))
                                                      (nippy/fast-freeze (assoc t :task/op :run))))))
                               (catch Throwable t
                                 (if exception-handler
                                   (exception-handler t)
                                   (.printStackTrace t))))))]
    (->Scheduler producer responses-worker make-handler-consumer-fn
                 executions-topic responses-topic sch-persistence
                 handler open-tasks nil name executor create-poll-task)))

(defn make-scheduler [consumer-id persistence {:keys [producer-fn consumer-fn name executor
                                                      producer-options consumer-options status-poll-interval-ms exception-handler
                                                      execution-topic-config response-topic-config]
                                               :or   {status-poll-interval-ms 5000
                                                      producer-fn             messaging/->producer
                                                      consumer-fn             messaging/->consumer}}]
  (let [execution-topic-config (merge {:name               "workflow-executions"
                                       :num-partitions     100
                                       :replication-factor 1}
                                      execution-topic-config)
        response-topic-config  (merge {:name               "workflow-responses"
                                       :num-partitions     1
                                       :replication-factor 1}
                                      response-topic-config)
        producer-options       (merge producer-options {"group.id" (str consumer-id)})
        _                      (let [adm (messaging/->admin (merge producer-options {"client.id" (str consumer-id "-admin")}))]
                                 (try
                                   @(messaging/create-topics-if-needed adm [execution-topic-config response-topic-config])
                                   (finally
                                     (messaging/close! adm))))
        p                      (producer-fn producer-options)
        c                      (consumer-fn (str consumer-id "-" (UUID/randomUUID)) consumer-options)]
    (make-scheduler* persistence p c #(consumer-fn consumer-id consumer-options)
                     {:executions-topic        (:name execution-topic-config)
                      :responses-topic         (:name response-topic-config)
                      :status-poll-interval-ms status-poll-interval-ms
                      :exception-handler       exception-handler
                      :executor                executor
                      :name                    name})))

(comment
  (messaging/close! (:scheduler fx))

  (keep #(let [after (:task/start-after %)]
           (when (and after (.isAfter (.toInstant after)
                                      (.toInstant #inst "2021-09-27T11:36:01.734-00:00")))
             (select-keys % [:task/id :task/execution-id :task/execution-options :task/response])))
        @(:state (:persistence (:scheduler fx))))

  (do
    (do
      (require '[workflow.interpreters :refer [->Sandboxed ->Naive]] :reload)
      (defn make [consumer-id options]
        (let [statem (mem/make-statem-persistence)]
          (wf/effects {:statem      statem
                       :execution   (mem/make-execution-persistence statem)
                       :scheduler   (make-scheduler consumer-id (mem/make-scheduler-persistence) options)
                       :interpreter (->Naive)})))
      (def fx (make "test_repl" {:status-poll-interval-ms 100
                                 :producer-options        {"bootstrap.servers" "localhost:9092"}
                                 :consumer-options        {"bootstrap.servers" "localhost:9092"}}))
      (wf/save-statem fx #:state-machine{:id             "order"
                                         :version        1
                                         :start-at       "create"
                                         :execution-mode "async-throughput"
                                         :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                                         :states         '{"create"    {:always [{:name  "created"
                                                                                  :state "cart"}]}
                                                           "cart"      {:actions {"add"    {:name    "added"
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
                                                           "submitted" {:actions {"fraud-approve" {:state "fraud-approved"}
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
      @(wf/save-statem fx #:state-machine{:id             "shipment"
                                          :version        1
                                          :start-at       "created"
                                          :execution-mode "async-throughput"
                                          :context        '{:id        "S1"
                                                            :order     (:order input)
                                                            :delivered false}
                                          :states         '{"created"     {:always [{:name  "fulfilled"
                                                                                     :state "outstanding"}]}
                                                            "outstanding" {:always  [{:id     "fetch"
                                                                                      :name   "fetched"
                                                                                      :invoke {:given (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}})
                                                                                               :if    (<= 200 (:status output) 299)
                                                                                               :then  {:state   "fetched"
                                                                                                       :context {:response {:n (:n (:json (:body output)))}}}
                                                                                               :else  {:state "failed"}}}]
                                                                           :actions {"cancel" {:state "canceled"}}}
                                                            "failed"      {:always [{:name     "retry"
                                                                                     :state    "outstanding"
                                                                                     :wait-for {:seconds 5}}]}

                                                            "canceled" {:end    true
                                                                        :return {:delivered false}}

                                                            "fetched" {:always [{:name    "deliver"
                                                                                 :state   "delivered"
                                                                                 :when    (> 3 (:n (:response state)))
                                                                                 :context {:response nil
                                                                                           :result   (:n (:response state))}}
                                                                                {:name     "retry"
                                                                                 :state    "outstanding"
                                                                                 :context  {:response nil}
                                                                                 :wait-for {:seconds 5}}]}

                                                            "delivered" {:end    true
                                                                         :return {:delivered true}}}})

      (protocol/register-execution-handler fx (wf/create-execution-handler fx))
      (def out (wf/start fx "order" nil)))

    (do
      (wf/trigger fx (second out) {::wf/action "add"
                                   ::wf/reply? true
                                   :sku        "bns12"
                                   :qty        1})
      #_(Thread/sleep 100)
      (wf/trigger fx (second out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                            ::wf/reply? true}))
      (async/take! res prn)))


  (messaging/close! (:scheduler fx))

  (messaging/metrics (:producer (:scheduler fx)))

  (do
    (clojure.pprint/print-table
     (sort-by
      (juxt :t)
      (map (fn [sm]
             (merge {:execution/event-name ""}
                    (select-keys (assoc sm :t [(or (:execution/step-started-at sm) (:execution/enqueued-at sm))
                                               (:execution/version sm)])
                                 [:execution/state-machine-id
                                  :execution/state
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
                                           (- (:execution/user-ended-at sm)
                                              (:execution/user-started-at sm))
                                           1000000)))}))
           (mapcat (fn [e] (wf/fetch-execution-history fx (:execution/id e)))
                   (wf/executions-for-statem fx "order" {:version :latest})))))

    (clojure.pprint/print-table
     (sort-by (fn [sm] [(or (:execution/step-started-at sm) (:execution/enqueued-at sm))
                        (:execution/version sm)])
              (wf/executions-for-statem fx "order" {:version :latest})))

    (doseq [[_ execs] (group-by
                       :execution/id
                       (sort-by (juxt :execution/state-machine-id :execution/version)
                                (wf/executions-for-statem fx "order" {:version :latest})))
            :let      [e (last execs)]]
      (println (:execution/state-machine-id e) "took" (double (/ (- (:execution/finished-at e) (:execution/enqueued-at e))
                                                                 1000000))
               "ms (enqueue->end)"
               (double (/ (- (:execution/finished-at e) (:execution/started-at e))
                          1000000))
               "ms (start->end)")))
  )