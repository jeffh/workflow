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

(defn- ^AttributeKey attr-key [k v]
  (cond
    (instance? AttributeKey k) k
    (string? v)                (AttributeKey/stringKey (name k))
    (double? v)                (AttributeKey/doubleKey (name k))
    (boolean? v)               (AttributeKey/booleanKey (name k))
    (integer? v)               (AttributeKey/longKey (name k))
    :else                      (throw (ex-info "Unsupported attribute value type" {:type  (type v)
                                                                                   :value v}))))

(defn ^Attributes attrs
  ([key value] (Attributes/of (attr-key key value) value))
  ([k1 v1 k2 v2] (Attributes/of (attr-key k1 v1) v2
                                (attr-key k2 v2) v2))
  ([k1 v1 k2 v2 k3 v3] (Attributes/of (attr-key k1 v1) v2
                                      (attr-key k2 v2) v2
                                      (attr-key k3 v3) v3))
  ([k1 v1 k2 v2 k3 v3 k4 v4] (Attributes/of (attr-key k1 v1) v2
                                            (attr-key k2 v2) v2
                                            (attr-key k3 v3) v3
                                            (attr-key k4 v4) v4))
  ([k1 v1 k2 v2 k3 v3 k4 v4 k5 v5] (Attributes/of (attr-key k1 v1) v2
                                                  (attr-key k2 v2) v2
                                                  (attr-key k3 v3) v3
                                                  (attr-key k4 v4) v4
                                                  (attr-key k5 v5) v5)))

(defn ^Span add-event
  ([span n] (.addEvent ^Span span (str n)))
  ([span n ^Attributes attributes] (.addEvent ^Span span (str n) attributes))
  ([span n ^Attributes attributes inst] (.addEvent ^Span span (str n) attributes inst)))
(defn set-attr-bool ^Span [^Span span ^String key value] (.setAttribute span key (boolean value)) span)
(defn set-attr-double ^Span [^Span span ^String key value] (.setAttribute span key (double (or value 0))) span)
(defn set-attr-long ^Span [^Span span ^String key value] (.setAttribute span key (long (or value 0))) span)
(defn set-attr-str ^Span [^Span span ^String key value] (.setAttribute span key (str value)) span)
(defn record-exception [^Span span ^Throwable t]
  (set-attr-str span "error.name" (str t))
  (.recordException span t))

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
      (set-attr-str ~span-sym "ns" ~*ns*)
      (set-attr-long ~span-sym "line" ~(:line md))
       (try
         ~@body
         (catch Throwable t#
           (record-exception ~span-sym t#)
           (throw t#))
         (finally
           (.close scope#)
           (end-span ~span-sym))))))
