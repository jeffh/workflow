(ns net.jeffhui.workflow.tracer
  (:import io.opentelemetry.api.GlobalOpenTelemetry
           io.opentelemetry.api.trace.Tracer
           io.opentelemetry.api.trace.Span
           io.opentelemetry.api.common.Attributes
           io.opentelemetry.api.common.AttributesBuilder
           io.opentelemetry.api.common.AttributeKey
           io.opentelemetry.context.Context))

;;;;;;;;;;;;;; Tap based tracing

(defn reset-taps []
  (swap! #'clojure.core/tapset empty))

(defmacro with-tap [f & body]
  `(let [f# ~f]
     (try
       (add-tap f#)
       ~@body
       (finally (remove-tap f#)))))

;;;;;;;;;;;;;;; OpenTelemetry based tracing

;; to enable, see https://opentelemetry.io/docs/instrumentation/java/manual_instrumentation/
;; and add: io.opentelemetry/opentelemetry-sdk {:mvn/version "1.10.0"}

(defn get-tracer [library]
  (GlobalOpenTelemetry/getTracer (str library)))

(def ^:dynamic *tracer* (delay (get-tracer "net.jeffhui.workflow.core")))

(defn current-span ^Span [] (Span/current))
(defn start-span ^Span
  ([^String name]
   (let [s (-> (.spanBuilder ^Tracer @*tracer* (str name))
               (.setParent (.with (Context/current) (Span/current)))
               .startSpan)]
     (.makeCurrent s)
     s))
  ([^Context parent ^String name]
   (let [s (-> (.spanBuilder ^Tracer @*tracer* (str name))
               (cond-> parent (.setParent (.with (Context/current) parent)))
               .startSpan)]
     (.makeCurrent s)
     s))
  ([^Tracer instance ^Context parent ^String name]
   (let [s (-> (.spanBuilder instance (str name))
               (cond-> parent (.setParent (.with (Context/current) parent)))
               .startSpan)]
     (.makeCurrent s)
     s)))
(defn end-span [^Span span] (.end span))

(defn add-event ^Span [^Span span name] (.addEvent span (str name)) span)
(defn set-attr-bool ^Span [^Span span ^String key value] (.setAttribute span key (boolean value)) span)
(defn set-attr-double ^Span [^Span span ^String key value] (.setAttribute span key (double value)) span)
(defn set-attr-long ^Span [^Span span ^String key value] (.setAttribute span key (long value)) span)
(defn set-attr-str ^Span [^Span span ^String key value] (.setAttribute span key (str value)) span)
(defn record-exception [^Span span ^Throwable t] (.recordException span t))

(defmacro with-span [[span-sym start-span-args] & body]
  `(let [~span-sym (start-span ~@(if (vector? start-span-args)
                                   start-span-args
                                   [start-span-args]))]
     (try
       ~@body
       (catch Throwable t#
         (record-exception ~span-sym t#)
         (throw t#))
       (finally
         (end-span ~span-sym)))))
