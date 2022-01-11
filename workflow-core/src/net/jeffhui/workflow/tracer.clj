(ns net.jeffhui.workflow.tracer
  (:import io.opentelemetry.api.GlobalOpenTelemetry
           io.opentelemetry.api.trace.Tracer
           io.opentelemetry.api.trace.Span
           io.opentelemetry.api.common.Attributes
           io.opentelemetry.api.common.AttributesBuilder
           io.opentelemetry.api.common.AttributeKey
           io.opentelemetry.context.Context))

;; to enable, see https://opentelemetry.io/docs/instrumentation/java/manual_instrumentation/
;; and add: io.opentelemetry/opentelemetry-sdk {:mvn/version "1.10.0"}

(def ^:dynamic *tracer*
  (delay (GlobalOpenTelemetry/getTracer "net.jeffhui.workflow.core")))

(defn current-span ^Span [] (Span/current))
(defn start-span ^Span
  ([^String name]
   (let [s (-> (.spanBuilder ^Tracer @*tracer*)
               (.setParent (Span/current))
               .startSpan)]
     (.makeCurrent s)
     s))
  ([^Context parent ^String name]
   (let [s (-> (.spanBuilder ^Tracer @*tracer*)
               (cond-> parent (.setParent parent))
               .startSpan)]
     (.makeCurrent s)
     s))
  ([^Tracer instance ^Context parent ^String name]
   (let [s (-> (.spanBuilder ^Tracer instance)
               (cond-> parent (.setParent parent))
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