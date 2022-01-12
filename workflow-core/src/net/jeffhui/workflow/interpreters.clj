(ns net.jeffhui.workflow.interpreters
  (:require [net.jeffhui.workflow.protocol :as p]
            [net.jeffhui.workflow.tracer :as tracer]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [sci.core :as sci]
            [jsonista.core :as json]))


(def ^:private options
  {:realize-max 100
   :load-fn     (constantly nil)})

(defn eval-action [edn-expr io context input output]
  (let [expr (pr-str edn-expr)]
    (tracer/with-span [sp "sci-eval"]
      (tracer/set-attr-str sp "expr" expr)
      (try
        (sci/eval-string expr
                         (assoc options :bindings
                                (merge {'io      io
                                        'ctx     context
                                        'input   input
                                        '*ctx*   context
                                        '*input* input}
                                       (when (not= ::p/nothing output)
                                         {'output   output
                                          '*output* output}))))
        (catch Exception e
          (tracer/record-exception sp e)
          (throw (ex-info "Failed to interpret action" {:code   edn-expr
                                                        :input  input
                                                        :output (when (not= ::p/nothing output)
                                                                  output)}
                          e)))))))

(defn clj-eval-action [edn-expr io context input output]
  (tracer/with-span [sp "clj-eval"]
    (tracer/set-attr-str sp "expr" (pr-str edn-expr))
    (try
      (eval `(let [~'io      ~io
                   ~'ctx     ~context
                   ~'input   ~input
                   ~'*io*    ~'io
                   ~'*ctx*   ~'ctx
                   ~'*input* ~'input]
               ~(if (= ::p/nothing output)
                  edn-expr
                  `(let [~'output   ~output
                         ~'*output* ~'output]
                     ~edn-expr))))
      (catch Exception e
        (tracer/record-exception sp e)
        (throw (ex-info "Failed to interpret action" {:code edn-expr} e))))))

(defrecord Sandboxed []
  p/MachineInterpreter
  (evaluate-expr [_ expr io context input output] (eval-action expr io context input output)))

(defrecord Naive []
  p/MachineInterpreter
  (evaluate-expr [_ expr io context input output] (clj-eval-action expr io context input output)))
