(ns net.jeffhui.workflow.interpreters
  (:require [net.jeffhui.workflow.protocol :as p]
            [clojure.string :as string]
            [clojure.edn :as edn]
            [sci.core :as sci]
            [jsonista.core :as json]))


(def eval-bindings
  {'str->double      (fn [x]
                       (try
                         (Double/parseDouble (str x))
                         (catch NumberFormatException _ nil)))
   'str->int         (fn [x]
                       (try
                         (Integer/parseInt (str x))
                         (catch NumberFormatException _ nil)))
   'str-join         string/join
   'str-contains?    string/includes?
   'str-starts-with? string/starts-with?
   'str-ends-with?   string/ends-with?
   'str-split        string/split
   'str-split-lines  string/split-lines
   'json-parse       (fn [v] (json/read-value v json/keyword-keys-object-mapper))
   'json-stringify   json/write-value-as-string
   'edn-parse        edn/read-string
   'edn-stringify    pr-str})

(def eval-nonterminating
  '[loop repeat deref recur letfn fn])

(def eval-allows
  (into
   '[let if cond and or not seq when when-not do
     persistent! transient conj! disj! assoc! dissoc! prn
     > >= = < <= not= zero? pos? neg? even? odd? true? false? nil?
     -> ->> cond-> as-> cond->> some-> some->> comp partial juxt fnil
     assoc assoc-in update update-in get get-in merge dissoc
     + - * / inc dec mod min max rem quot
     int integer?
     double double?
     str string?
     pr-str prn-str
     char char?
     keyword?
     sequential? list? count map? set? vector? vector empty not-empty into conj disj cons distinct? distinct set
     concat flatten partition partition-all partition-by sort sort-by shuffle apply
     frequencies group-by line-seq re-seq take take-last take-nth drop drop-last
     first second last rest nth peek pop
     keys vals
     format
     rand rand-int rand-nth random-sample
	 println print
    ;; ;; imports from another namespace
    ;;  str-join str-includes? str-starts-with? str-ends-with? str-split str-split-lines
    ;; ;; custom
    ;;  str->double str->int
     ]
   (keys eval-bindings)))

(def ^:private allowed
  (-> eval-allows
      (into eval-nonterminating)
      (into '[io ctx input output *ctx* *input* *output*])))

(def ^:private options
  {:allow       allowed
   :realize-max 100
   :namespaces  {}
   :load-fn     (constantly nil)})

(defn eval-action [edn-expr io context input output]
  (try
    (sci/eval-string (pr-str edn-expr)
                     (assoc options :bindings
                            (merge eval-bindings
                                   {'io      io
                                    'ctx     context
                                    'input   input
                                    '*ctx*   context
                                    '*input* input}
                                   (when (not= ::p/nothing output)
                                     {'output   output
                                      '*output* output}))))
    (catch Exception e
      (throw (ex-info "Failed to interpret action" {:code   edn-expr
                                                    :input  input
                                                    :output (when (not= ::p/nothing output)
                                                              output)}
                      e)))))

(defn clj-eval-action [edn-expr io context input output]
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
      (throw (ex-info "Failed to interpret action" {:code edn-expr} e)))))

(defrecord Sandboxed []
  p/MachineInterpreter
  (evaluate-expr [_ expr io context input output] (eval-action expr io context input output)))

(defrecord Naive []
  p/MachineInterpreter
  (evaluate-expr [_ expr io context input output] (clj-eval-action expr io context input output)))
