(ns net.jeffhui.workflow.impl.kafka.messaging
  (:require [taoensso.nippy :as nippy]
            [clojure.string :as string])
  (:import org.apache.kafka.common.serialization.Serdes
           org.apache.kafka.common.serialization.Serializer
           org.apache.kafka.common.serialization.Deserializer
           org.apache.kafka.common.TopicPartition
           org.apache.kafka.common.KafkaFuture
           org.apache.kafka.common.KafkaFuture$BaseFunction
           org.apache.kafka.common.errors.WakeupException
           org.apache.kafka.common.MetricName
           org.apache.kafka.common.Metric
           org.apache.kafka.common.header.Header
           org.apache.kafka.clients.producer.Producer
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.producer.MockProducer
           org.apache.kafka.clients.producer.Partitioner
           org.apache.kafka.clients.producer.ProducerRecord
           org.apache.kafka.clients.producer.RecordMetadata
           org.apache.kafka.clients.consumer.KafkaConsumer
           org.apache.kafka.clients.consumer.MockConsumer
           org.apache.kafka.clients.consumer.ConsumerRecord
           org.apache.kafka.clients.consumer.OffsetAndMetadata
           org.apache.kafka.clients.consumer.OffsetResetStrategy
           org.apache.kafka.clients.admin.KafkaAdminClient
           org.apache.kafka.clients.admin.Admin
           org.apache.kafka.clients.admin.TopicListing
           org.apache.kafka.clients.admin.NewTopic
           java.time.Duration
           java.util.Collection
           java.util.Map))

(set! *warn-on-reflection* true)

(defn serdes [encode decode]
  (Serdes/serdeFrom
   (reify Serializer (serialize [_ topic data] (encode topic data)))
   (reify Deserializer (deserialize [_ topic data] (decode topic data)))))

(def nippy-serdes
  "Provides a serializer of clojure data through kafka using nippy"
  (serdes #(nippy/fast-freeze %2) #(nippy/fast-thaw %2)))

(defn- ->header [^String k ^bytes v]
  (let [v (cond
            (bytes? v) v
            (string? v) (.getBytes ^String v)
            :else (byte-array v))]
    (reify org.apache.kafka.common.header.Header
      (key [_] k)
      (value [_] v))))

(defrecord Message [topic key value offset partition timestamp headers])

(defn- RecordMetadata->map [^RecordMetadata rm]
  (merge {:partition       (.partition rm)
          :topic           (.topic rm)
          :key-num-bytes   (.serializedKeySize rm)
          :value-num-bytes (.serializedValueSize rm)}
         (when (.hasOffset rm) {:offset (.offset rm)})
         (when (.hasTimestamp rm) {:timestamp (.timestamp rm)})))

(defn- ProducerRecord->Message [^ProducerRecord record]
  (Message. (.topic record)
            (.key record)
            (.value record)
            nil
            (.partition record)
            (.timestamp record)
            (into []
                  (map (fn [^Header h] [(.key h) (.value h)]))
                  (.headers record))))

(defn- ConsumerRecord->Message [^ConsumerRecord record]
  (Message. (.topic record)
            (.key record)
            (.value record)
            (.offset record)
            (.partition record)
            (.timestamp record)
            (into []
                  (map (fn [^Header h] [(.key h) (.value h)]))
                  (.headers record))))

(defn ^Admin ->admin [config]
  (Admin/create ^Map (merge {"client.id" "net-jeffhui-workflow-admin"}
                            config)))

(defn- ^KafkaFuture$BaseFunction ->BaseFunction [f]
  (reify KafkaFuture$BaseFunction
    (apply [_ a] (f a))))

(defn- then-true [^KafkaFuture fut]
  (.thenApply fut (->BaseFunction (constantly true))))

(defn list-topics [^Admin adm]
  @(.thenApply (.listings (.listTopics adm))
               (->BaseFunction
                (partial map (fn [^TopicListing tl]
                               {:name      (.name tl)
                                :internal? (.isInternal tl)})))))

(defn metric-name->map [^MetricName mname]
  {:name        (.name mname)
   :group       (.group mname)
   :description (.description mname)
   :tags        (into {} (.tags mname))})

(defn metrics [adm-or-consumer-or-producer]
  (into {}
        (map (fn [[^MetricName mname ^Metric metric]]
               [[(.group mname) (.name mname)]
                {:metric  (metric-name->map (.metricName metric))
                 :value (.metricValue metric)}]))
        (cond
          (instance? KafkaAdminClient adm-or-consumer-or-producer)
          (.metrics ^KafkaAdminClient adm-or-consumer-or-producer)

          (instance? Producer adm-or-consumer-or-producer)
          (.metrics ^Producer adm-or-consumer-or-producer)

          (instance? KafkaConsumer adm-or-consumer-or-producer)
          (.metrics ^KafkaConsumer adm-or-consumer-or-producer))))

(defn create-topics [^Admin adm topic-configs]
  (then-true
   (.all
    (.createTopics adm
                   (map (fn [t] (doto (NewTopic. (str (:name t))
                                                 (int (:num-partitions t 1))
                                                 (short (:replication-factor t 1)))
                                  (.configs (:options t))))
                        topic-configs)))))

(defn create-topics-if-needed [^Admin adm topic-configs]
  (let [existing-topics (set (map :name (list-topics adm)))]
    (->> topic-configs
         (remove (comp existing-topics :name))
         (create-topics adm))))

(defn delete-topics [^Admin adm ^java.util.Collection topics]
  (then-true (.all (.deleteTopics adm topics))))

(defn ^MockProducer ->mock-producer [{:keys [autocomplete? num-partitions
                                             key-serializer value-serializer]
                                      :or   {autocomplete?   true
                                             num-partitions  1
                                             key-serializer  (.serializer (Serdes/String))
                                             value-serialize (.serializer (Serdes/ByteArray))}}]
  (MockProducer. (boolean autocomplete?)
                 (reify Partitioner
                   (partition [_ topic key keyBytes value valueBytes cluster]
                     (mod (hash key) num-partitions))
                   (close [_]))
                 key-serializer
                 value-serializer))

(defn mock-producer-history [^MockProducer mp] (map ProducerRecord->Message (.history mp)))
(defn mock-producer-partition-history [^MockProducer mp]
  (.consumerGroupOffsetsHistory mp))
(defn mock-producer-clear-history [^MockProducer mp] (.clear mp))
(defn mock-producer-complete-next [^MockProducer mp]
  (.completeNext mp))
(defn mock-producer-error-next [^MockProducer mp exception]
  (.errorNext mp exception))

(defn ^KafkaProducer ->producer [config]
  (KafkaProducer. ^Map (merge {"enable.idempotence" "true"
                               "acks"               "all"
                               "compression.type"   "snappy"
                               "key.serializer"     "org.apache.kafka.common.serialization.StringSerializer"
                               "value.serializer"   "org.apache.kafka.common.serialization.ByteArraySerializer"}
                              config)))

(defn producer-enable-transactions! [^Producer p]
  (.initTransactions p)
  p)

(defn send!
  "Sends a sequence of messages through a given producer.

  Parameters:
  - producer :: KafkaProducer
      The producer to send the messages through.
  - messages :: seq of tuples [topic :: String, key :: Any value :: Any]
      The messages to send through. Each message is a tuple of topic, key, and
      value. Topic must be a string, but key and value should be any value
      supported by the serializers of the producer.

      For more information, see \"key.serializer\" and \"value.serializer\"
      configuration.
  - headers :: a map of key value pairs where keys are strings, and values are String or bytes
  "
  ([^Producer producer topic key value]
   (future
     (RecordMetadata->map
      @(.send producer (ProducerRecord. (str topic) key value)))))
  ([^Producer producer topic key value headers]
   (future
     (let [^Integer partition nil]
       (RecordMetadata->map @(.send producer (ProducerRecord. (str topic) partition key value ^Iterable (map (fn [[k v]] (->header k v)) headers)))))))
  ([^Producer producer topic key value headers ^Integer partition]
   (future
     (RecordMetadata->map
      @(.send producer (ProducerRecord. (str topic) partition key value ^Iterable (map (fn [[k v]] (->header k v)) headers))))))
  ([^Producer producer messages]
   (future
     (let [responses
           (vec ;; realize
            (for [[topic key value headers ^Integer partition] messages]
              (.send producer (ProducerRecord. (str topic) partition key value ^Iterable (map (fn [[k v]] (->header k v)) headers)))))]
       (.flush producer)
       (mapv #(RecordMetadata->map (deref %)) responses)))))

#_(defn send-tx!
    "Sends a sequence of messages through a given producer.

  Parameters:
  - producer :: KafkaProducer
      The producer to send the messages through.
  - messages :: seq of tuples [topic :: String, key :: Any value :: Any]
      The messages to send through. Each message is a tuple of topic, key, and
      value. Topic must be a string, but key and value should be any value
      supported by the serializers of the producer.

      For more information, see \"key.serializer\" and \"value.serializer\"
      configuration.
  "
    [^KafkaProducer producer messages]
    (try
      (.beginTransaction producer)
      (doseq [[topic key value] messages]
        (.send producer (ProducerRecord. (str topic) key value)))
      (.commitTransaction producer)
      (catch ProducerFencedException e
        (.close producer)
        (throw e))
      (catch OutOfOrderSequenceException e
        (.close producer)
        (throw e))
      (catch AuthorizationException e
        (.close producer)
        (throw e))
      (catch KafkaException e
        (.abortTransaction producer)))
    (.flush producer))

(defn ^MockConsumer ->mock-consumer
  ([_group-id config] (->mock-consumer config)) ;; emulate the ->consumer interface
  ([options] (MockConsumer. (OffsetResetStrategy/valueOf (string/upper-case (str (or (get options "auto.offset.reset")
                                                                                     "earliest")))))))

(defn mock-consumer-set-poll-exception [^MockConsumer mc exception]
  (.schedulePollTask mc (fn [] (.setPollException mc exception))))

(defn mock-consumer-set-beginning-offsets [^MockConsumer mc partition->offsets]
  (.updateBeginningOffsets mc (into {}
                                    (map (fn [[[topic partition] offset]]
                                           [(TopicPartition. (str topic) partition)
                                            offset]))
                                    partition->offsets)))

(defn mock-consumer-add-records [^MockConsumer mc records]
  (let [records (mapv (fn [{:keys [topic partition offset key value]} records]
                        (ConsumerRecord. (str topic) (int partition) (long offset) key value))
                      records)]
    (.schedulePollTask mc (fn [] (doseq [r records] (.addRecord mc r))))))

(defn ^KafkaConsumer ->consumer [group-id config]
  (KafkaConsumer. ^Map (merge {"enable.auto.commit" "false"
                               "auto.offset.reset"  "earliest"
                               "isolation.level"    "read_committed"
                               "key.deserializer"   "org.apache.kafka.common.serialization.StringDeserializer"
                               "value.deserializer" "org.apache.kafka.common.serialization.ByteArrayDeserializer"
                               "group.id"           (str group-id)}
                              config)))

(defn ^KafkaConsumer subscribe [^KafkaConsumer consumer topics]
  (.subscribe consumer ^Collection (set topics))
  consumer)

(defn ^KafkaConsumer unsubscribe-all [^KafkaConsumer consumer]
  (.unsubscribe consumer)
  consumer)

(defn poll [^KafkaConsumer consumer max-wait-in-millis]
  (mapv ConsumerRecord->Message
        (.poll consumer (Duration/ofMillis (long max-wait-in-millis)))))

(defn commit-offsets [^KafkaConsumer consumer partition->next-offset]
  (.commitSync consumer ^Map (into {}
                                   (map (fn [[[topic partition] offset]]
                                          [(TopicPartition. topic partition) (OffsetAndMetadata. offset)]))
                                   partition->next-offset)))

(defn- try-safely [f record]
  (try
    {:input  record
     :result (f record)}
    (catch WakeupException e
      ;; TODO: partial committing of offsets is probably useful!
      (throw e))
    (catch Exception e
      {:input     record
       :exception e})))

(defn- try-until [f pred records]
  (let [size    (count records)
        records (vec records)]
    (loop [index 0
           accum (transient [])]
      (if (<= size index)
        (persistent! accum)
        (let [result (try-safely f (records index))]
          (if (pred result)
            (persistent! (conj! accum result))
            (recur (inc index)
                   (conj! accum result))))))))

(defn- ->TopicPartitions [partitions]
  (map (fn [[topic partition-key]]
         (TopicPartition. (str topic) partition-key))
       partitions))

(defn pause-partitions [^KafkaConsumer consumer partitions]
  (.pause consumer (->TopicPartitions partitions)))

(defn resume-partitions [^KafkaConsumer consumer partitions]
  (.resume consumer (->TopicPartitions partitions)))

(defn optimistic-processor
  "Commits offsets regardless if the processing function succeeds or fails.
  Never will get stuck, but may skip messages that throw exceptions.
  "
  [processor fail-handler messages]
  (->> messages
       (map (partial try-safely processor))
       (map #(do (when (and fail-handler (:exception %)) (fail-handler %)) %))
       (reduce (fn [key->offsets {:keys [input]}]
                 (update key->offsets [(:topic input) (:partition input)] (fnil max 0) (inc (:offset input))))
               {})))

(defn pessimistic-processor
  "Commits offsets only up to the most recent, successfully processed message.
  Can become 'stuck' if a message causes an exception, but ensures every message
  must be processed without exceptions to proceed.
  "
  [processor fail-handler messages]
  (let [results (try-until processor :exception messages)]
    (when-let [error-result (first (filter :exception results))]
      (when fail-handler
        (fail-handler error-result)))
    (reduce (fn [key->offsets {:keys [input]}]
              (update key->offsets [(:topic input) (:partition input)] (fnil max 0) (inc (:offset input))))
            {}
            (remove :exception results))))

(defn TopicPartition->map [^TopicPartition tp]
  {:topic     (.topic tp)
   :partition (.partition tp)})

(defn consumer-assignment [^KafkaConsumer consumer]
  (into #{} (map TopicPartition->map (.assignment consumer))))

(defn consumer-loop [^KafkaConsumer consumer closed-atom {:keys [poll-duration
                                                                 topics
                                                                 value-deserializer
                                                                 processing-method
                                                                 process-message
                                                                 failed-message-handler
                                                                 exception-handler]
                                                          :or   {poll-duration      10000
                                                                 processing-method  optimistic-processor
                                                                 value-deserializer nippy/fast-thaw}}]
  (assert (ifn? process-message) "process-message must be a function")
  (assert (not-empty topics) "topics must be provided")
  (assert (or (nil? failed-message-handler) (ifn? failed-message-handler))
          "failed-message-handler must be nil or a function")
  (assert (or (nil? exception-handler) (ifn? exception-handler))
          "exception-handler must be nil or a function")
  (assert (ifn? value-deserializer)
          "value-deserializer must be a function")
  (let [msg-processor (comp process-message (fn [m] (update m :value value-deserializer)))
        thread-name   (.getName (Thread/currentThread))]
    (try
      (subscribe consumer topics)
      #_(locking *out* (println "Started Consumer Loop" thread-name topics))
      (while (not @closed-atom)
        (let [records (poll consumer poll-duration)
              commits (processing-method msg-processor
                                         failed-message-handler
                                         records)]
          (commit-offsets consumer (not-empty commits))))
      (catch WakeupException e nil)
      (catch Exception e
        (if exception-handler
          (exception-handler e)
          (.printStackTrace e)))
      (finally
        #_(locking *out* (println "Consumer Exit" thread-name))
        (.close consumer)))))

(defn backgrounded-consumer
  "Process messages from consumer via process-message functino on a background thread.

  This function assumes ownership of consumer. This means on calling the
  cancellation function, this consumer will be closed.

  process-message = (fn [^Message record] ...)
  "
  [^KafkaConsumer consumer {:keys [poll-duration
                                   topics
                                   value-deserializer
                                   processing-method
                                   process-message
                                   failed-message-handler
                                   exception-handler
                                   thread-name]
                            :or   {poll-duration      10000
                                   processing-method  optimistic-processor
                                   value-deserializer nippy/fast-thaw}
                            :as   options}]
  (assert (ifn? process-message) "process-message must be a function")
  (assert (not-empty topics) "topics must be provided")
  (assert (or (nil? failed-message-handler) (ifn? failed-message-handler))
          "failed-message-handler must be nil or a function")
  (assert (or (nil? exception-handler) (ifn? exception-handler))
          "exception-handler must be nil or a function")
  (assert (ifn? value-deserializer)
          "value-deserializer must be a function")
  (let [closed (atom false)
        t      (doto (Thread. (fn [] (consumer-loop consumer closed options))
                              (format "kafka-consumer[%s]-%s" (string/join "," topics)
                                      (str thread-name)))
                 (.start))]
    (fn closer []
      (reset! closed true)
      (.wakeup consumer))))

;;;;;;;;;;;;;;;;;;;;;;;;;

(defn close! [obj]
  (when obj
    (cond (instance? java.lang.AutoCloseable obj) (.close ^java.lang.AutoCloseable obj)
          (instance? java.io.Closeable obj)       (.close ^java.io.Closeable obj)
          (fn? obj)                               (obj)
          :else                                   (throw (IllegalArgumentException. (str "Don't know how to close: " obj))))))

#_(defstate producer
    :start (->producer {"bootstrap.servers"  "localhost:9092"})
    :stop (close! producer))

#_(defstate consumer
    :start (->consumer "workflow-dev-consumer" {"bootstrap.servers" "localhost:9092"})
    :stop (close! consumer))

#_(defn- print-message [m]
  (let [m (update m :value nippy/fast-thaw)]
    (println "RECEIVE:" (pr-str m))))

#_(defstate bg-consumer
  :start (-> (->consumer "workflow-bg-consumer" {"bootstrap.servers" "localhost:9092"})
             (backgrounded-consumer {:topics            ["test_topic"]
                                     :processing-method pessimistic-processor
                                     :process-message   print-message
                                     :exception-handler (fn [^Exception e] (.printStackTrace e))}))
  :stop (close! bg-consumer))

#_(defstate admin
  :start (->admin {"bootstrap.servers" "localhost:9092"})
  :stop (close! admin))

(comment
  (def admin (->admin {"bootstrap.servers" "localhost:9092"}))

  (list-topics admin)

  @(delete-topics admin ["test_topic"])

  (def r (create-topics admin [{:name "test_topic" :num-partitions 24 :replication-factor 1}]))

  (deref r 5000 :timeout)

  (metrics admin)

  (def producer ())

  (let [m {:time (.getTime (java.util.Date.))
           :yo   "lo"}]
    (prn "SEND" m)
    (send! producer "test_topic" "test" (nippy/fast-freeze m)))

  (get (metrics producer) ["producer-metrics" "requests-in-flight"])
  (get (metrics producer) ["producer-node-metrics" "request-total"])

  (make-topology
   [["test-input" "test-output" (fn [s]
                                  (-> s
                                      (kmap-value (fn [v] v))))]])
  )
