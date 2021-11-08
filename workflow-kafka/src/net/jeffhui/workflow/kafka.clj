(ns net.jeffhui.workflow.kafka
  (:require [net.jeffhui.workflow.protocol :as protocol]
            [net.jeffhui.workflow.impl.kafka.messaging :as messaging]
            [net.jeffhui.workflow.memory :as mem]
            [net.jeffhui.workflow.api :as wf]
            [taoensso.nippy :as nippy]
            [clojure.core.async :as async]
            [net.jeffhui.workflow.api :as api]
            [clojure.set :as set])
  (:import java.util.Date
           java.util.UUID
           java.time.Instant
           java.util.concurrent.Executors
           java.util.concurrent.ExecutorService))

(defmacro ^:private try? [& body]
  `(try ~@body (catch Throwable t# nil)))

(defn- execution-handler [producer persistence handler open-tasks ^ExecutorService pool msg]
  ;; limitations:
  ;;   open-tasks should only be called on completion ops - we need a completion message on a topic
  (let [task (:value msg)]
    ;; (println (format "ExecutionHandler(%s)" (pr-str task)))
    (when-let [f (first @handler)]
      (let [res (f (:task/execution-id task) (:task/execution-input task))]
        (when (:task/start-after task)
          (protocol/complete-task persistence (:task/id task) res))

        (when-let [response-topic (:task/reply-topic task)]
          (let [next-task (assoc task :task/response res)]
            (messaging/send! producer response-topic (str (:task/execution-id task))
                             (nippy/fast-freeze next-task))))))))

(defn- responses-handler [producer persistence handler open-tasks msg]
  ;; limitations:
  ;;   open-tasks should only be called on completion ops - we need a completion message on a topic
  (let [task (:value msg)]
    (when-let [ch (get @open-tasks (:task/id task))]
      ;; (println (format "MessageHandler(%s, %s)" (pr-str task) (pr-str ch)))
      (when-let [res (:task/response task)]
        (async/put! ch res))
      (async/close! ch)
      (swap! open-tasks dissoc (:task/id task))
      (try? (protocol/delete-task persistence (:task/id task))))))

(defrecord Scheduler [producer responses-worker make-producer make-responses-consumer make-execution-consumer executions-topic responses-topic persistence handler open-tasks close-poller name-hint ^ExecutorService executor create-poll-task make-executor]
  protocol/Scheduler
  (sleep-to [this timestamp execution-id options]
    (let [reply                  (when (::wf/reply? options) (async/chan 3))
          options                (dissoc options ::wf/reply?)
          task-id                (str "stask_" (UUID/randomUUID))
          ;; TODO(jeff): we probably shouldn't be obligated to have a chan for async replies
          _                      (swap! open-tasks assoc task-id (or reply (async/chan 1)))
          {error :error :as res} @(protocol/save-task persistence #:task{:id              task-id
                                                                         :execution-id    execution-id
                                                                         :execution-input options
                                                                         :start-after     timestamp
                                                                         :reply-topic     responses-topic})]
      (if error
        (do
          (when reply
            (async/put! reply res)
            (async/close! reply))
          (swap! open-tasks dissoc task-id)
          reply)
        reply)))
  (enqueue-execution [_ execution-id options]
    (let [reply   (when (::wf/reply? options) (async/chan 1))
          options (dissoc options ::wf/reply?)
          ;; "rtask" = "realtime task"
          task-id (str "rtask_" (UUID/randomUUID))]
      (when reply (swap! open-tasks assoc task-id reply))
      (messaging/send! producer executions-topic (str execution-id)
                       (nippy/fast-freeze #:task{:id              task-id
                                                 :execution-id    execution-id
                                                 :execution-input options
                                                 :reply-topic     (when reply responses-topic)}))
      reply))
  (register-execution-handler [_ f]
    (if (and (nil? (second @handler)) f)
      (let [c (make-execution-consumer)]
        (reset! handler [f (messaging/backgrounded-consumer
                            c
                            {:topics          #{executions-topic}
                             :thread-name     name-hint
                             :process-message (partial execution-handler producer persistence handler open-tasks executor)})]))
      (swap! handler assoc 0 f))
    true)
  protocol/Connection
  (open* [this]
    (let [this'    (update (protocol/close this) :persistence protocol/open)
          producer (make-producer)]
      (assoc this'
             :close-poller (create-poll-task producer)
             :executor (make-executor)
             :producer producer
             :responses-worker (messaging/backgrounded-consumer
                                (make-responses-consumer)
                                {:topics          #{responses-topic}
                                 :thread-name     name-hint
                                 :process-message (partial responses-handler producer (:persistence this') handler open-tasks)}))))
  (close* [this]
    (protocol/register-execution-handler this nil)
    (try? (when close-poller (close-poller)))
    (try? (when-let [worker (second @handler)] (messaging/close! worker)))
    (try? (messaging/close! responses-worker))
    (try? (messaging/close! producer))
    (try? (when executor (.shutdown executor)))
    (assoc this
           :close-poller nil
           :worker nil
           :responses-worker nil
           :producer nil
           :executor nil
           :persistence (protocol/close persistence))))

;; TODO(jeff): rename status-poll-interval-ms to something more API-consumer centric - perhaps recovery-poll-interval-ms?
(defn make-scheduler* [sch-persistence make-producer make-responses-consumer make-handler-consumer
                       {:keys [executions-topic responses-topic name
                               status-poll-interval-ms exception-handler
                               make-executor]
                        :or   {executions-topic        "workflow-executions"
                               responses-topic         "workflow-responses"
                               status-poll-interval-ms 5000}}]
  (assert (>= status-poll-interval-ms 100)
          (format "Low poll intervals are not allows for recovery fetches (since it introduces bugs): got %d" status-poll-interval-ms))
  (let [make-executor    (or make-executor #(Executors/newCachedThreadPool (protocol/thread-factory (constantly "wf-kafka-scheduler"))))
        open-tasks       (atom {})
        handler          (atom nil)
        responses-worker nil
        ;; TODO(jeff): each topic needs its own consumer, response-topic-config needs unique consumer group per every listener
        create-poll-task (fn create-poll-task [producer]
                           (reset! open-tasks {})
                           (let [seen-tasks (atom [])]
                             (protocol/schedule-recurring-local-task!
                              status-poll-interval-ms status-poll-interval-ms
                              (fn []
                                (try
                                  (when-let [open-tasks (not-empty (keys @open-tasks))]
                                    ;; TODO(jeff): we need to have only one poller, globally
                                    (let [outstanding-tasks (filter (comp (set/difference (set open-tasks)
                                                                                          (set @seen-tasks))
                                                                          :task/id)
                                                                    (protocol/runnable-tasks sch-persistence (Date/from (Instant/now))))]
                                      (doseq [t     outstanding-tasks
                                              :when (not (:task/complete? t))]
                                        (swap! seen-tasks conj (:task/id t))
                                        (swap! seen-tasks (fn [items]
                                                            (let [max-items 1000]
                                                              (if (< (count items) max-items)
                                                                items
                                                                (vec
                                                                 (drop (- (count items) max-items)
                                                                       items))))))

                                        (messaging/send! producer
                                                         executions-topic
                                                         (str (:task/execution-id t))
                                                         (nippy/fast-freeze t)))))
                                  (catch Throwable t
                                    (if exception-handler
                                      (exception-handler t)
                                      (.printStackTrace t))))))))]
    (->Scheduler nil nil make-producer make-responses-consumer make-handler-consumer
                 executions-topic responses-topic sch-persistence
                 handler open-tasks nil name nil create-poll-task make-executor)))

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
        producer-options       (merge producer-options {"group.id" (str consumer-id "-producer")})
        _                      (let [adm (messaging/->admin (merge producer-options {"client.id" (str consumer-id "-admin")}))]
                                 (try
                                   @(messaging/create-topics-if-needed adm [execution-topic-config response-topic-config])
                                   (finally
                                     (messaging/close! adm))))]
    (make-scheduler* persistence
                     #(producer-fn producer-options)
                     #(consumer-fn (str consumer-id "-responses-" (UUID/randomUUID)) consumer-options)
                     #(consumer-fn (str consumer-id "-executor") consumer-options)
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
             (select-keys % [:task/id :task/execution-id :task/execution-input :task/response])))
        @(:state (:persistence (:scheduler fx))))

  (do
    (do
      (require '[net.jeffhui.workflow.interpreters :refer [->Sandboxed ->Naive]] :reload)
      (require '[net.jeffhui.workflow.contracts :as contracts] :reload)
      (defn make [consumer-id options]
        (wf/effects {:statem      (mem/make-statem-persistence)
                     :execution   (mem/make-execution-persistence)
                     :scheduler   (make-scheduler consumer-id (mem/make-scheduler-persistence) options)
                     :interpreter (->Naive)}))
      (def fx (api/open (make "test_repl" {:status-poll-interval-ms 100
                                           :producer-options        {"bootstrap.servers" "localhost:9092"}
                                           :consumer-options        {"bootstrap.servers" "localhost:9092"}})))
      (wf/save-statem fx contracts/prepare-cart-statem)
      (wf/save-statem fx contracts/order-statem)
      (wf/save-statem fx contracts/shipment-statem)

      (protocol/register-execution-handler fx (wf/create-execution-handler fx))
      (def out (wf/start fx "order" nil)))

    (do
      (def ch (wf/trigger fx (second out) {::wf/action "add"
                                           ::wf/reply? true
                                           :sku        "bns12"
                                           :qty        1}))

      #_(Thread/sleep 100)
      (wf/trigger fx (second out) {::wf/action "place"})
      #_(Thread/sleep 100)
      (def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                            ::wf/reply? true}))
      (async/take! res prn)))

  (some wf/execution-error-truncated
        (wf/fetch-execution-history fx (second out)))

  (wf/fetch-execution-history fx #uuid "721eb2a9-f537-4a69-bb05-ce09a27fa6d5")

  (async/<!! ch)


  (wf/close fx)


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
                                  :execution/pause-state
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
