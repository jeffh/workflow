(ns net.jeffhui.workflow.tracer
  (:import io.opentelemetry.api.GlobalOpenTelemetry
           io.opentelemetry.api.OpenTelemetry
           io.opentelemetry.api.trace.Tracer
           io.opentelemetry.api.trace.Span
           io.opentelemetry.context.Scope
           io.opentelemetry.api.common.Attributes
           io.opentelemetry.api.common.AttributesBuilder
           io.opentelemetry.api.common.AttributeKey
           io.opentelemetry.context.Context))

;;;;;;;;;;;;;; Tap based tracing

(defn reset-taps
  "A simple helper function to reset clojure.core's register taps that were added via [[add-tap]]"
  []
  (swap! @#'clojure.core/tapset empty))

(defmacro with-tap [f & body]
  `(let [f# ~f]
     (try
       (add-tap f#)
       ~@body
       (finally (remove-tap f#)))))

;;;;;;;;;;;;;;; OpenTelemetry based tracing

;; to enable, see https://opentelemetry.io/docs/instrumentation/java/manual_instrumentation/
;; and add: io.opentelemetry/opentelemetry-sdk {:mvn/version "1.10.0"}

(def ^:private tracer (atom nil))
(defn set-tracer! [^OpenTelemetry instance]
  (reset! tracer instance))

(defn get-tracer [library]
  (.get (.getTracerProvider ^OpenTelemetry (or @tracer (GlobalOpenTelemetry/get))) (str library)))

(def core-tracer "net.jeffhui.workflow.core")

(defn current-span ^Span [] (Span/current))
(defn start-span ^Span
  ([^String name]
   (let [s (-> (.spanBuilder ^Tracer (get-tracer core-tracer) (str name))
               (.setParent (.with (Context/current) (Span/current)))
               .startSpan)]
     s))
  ([^Context parent ^String name]
   (let [s (-> (.spanBuilder ^Tracer (get-tracer core-tracer) (str name))
               (cond-> parent (.setParent (.with (Context/current) parent)))
               .startSpan)]
     s))
  ([^Tracer instance ^Context parent ^String name]
   (let [s (-> (.spanBuilder instance (str name))
               (cond-> parent (.setParent (.with (Context/current) parent)))
               .startSpan)]
     s)))
(defn end-span [^Span span]
  (.end span))

(defmacro ^Span add-event
  ([span name] `(.addEvent ^Span ~span (str ~name)))
  ([span name attributes]
   `(.addEvent ^Span ~span (str ~name)
               (Attributes/of
                ~@(mapcat
                   identity
                   (for [[k v] attributes]
                     (cond
                       (instance? AttributeKey k) [k v]
                       (= str (first v)) [(AttributeKey/stringKey k) v]
                       (= double (first v)) [(AttributeKey/doubleKey k) v]
                       (= boolean (first v)) [(AttributeKey/booleanKey k) v]
                       (#{long int} (first v)) [(AttributeKey/longKey k) v]
                       :else (throw (ex-info "Unsupported attribute value type" {:type (type v)
                                                                                 :value v})))))))))
(defn set-attr-bool ^Span [^Span span ^String key value] (.setAttribute span key (boolean value)) span)
(defn set-attr-double ^Span [^Span span ^String key value] (.setAttribute span key (double (or value 0))) span)
(defn set-attr-long ^Span [^Span span ^String key value] (.setAttribute span key (long (or value 0))) span)
(defn set-attr-str ^Span [^Span span ^String key value] (.setAttribute span key (str value)) span)
(defn record-exception [^Span span ^Throwable t] (.recordException span t))

(defmacro with-span
  ;; Usages:
  ;;   (with-span [span [& span-args]]
  ;;     ...)
  ;;   (with-span tracer [span [& span-args]]
  ;;     ...)
  [& args]
  (let [[tracer [span-sym start-span-args] body]
        (if (vector? (first args))
          [nil (first args) (rest args)]
          [(first args) (second args) (rest (rest args))])
        md (meta &form)]
    (assert span-sym)
    (assert start-span-args)
    `(let [~span-sym (start-span (or ~tracer (get-tracer core-tracer))
                                 ~@(if (vector? start-span-args)
                                     start-span-args
                                     [`(Span/current) start-span-args]))
           scope# (.makeCurrent ~span-sym)]
      (set-attr-str ~span-sym "ns" *ns*)
      (set-attr-long ~span-sym "line" ~(:line md))
       (try
         ~@body
         (catch Throwable t#
           (record-exception ~span-sym t#)
           (throw t#))
         (finally
           (.close scope#)
           (end-span ~span-sym))))))
