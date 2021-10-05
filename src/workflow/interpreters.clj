(ns workflow.interpreters
  (:require [workflow.protocol :as p]
            [workflow.api :as wf]
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
   '[let if cond and or not
     persistent! transient conj! disj! assoc! dissoc!
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

(defn eval-action [edn-expr io state input output]
  (letfn [(const-state [form]
            (cond (map? form)        (if-let [state (:state form)]
                                       {:failed? (not (string? state))
                                        :value   state}
                                       (reduce (fn [a [k v]] (or a (const-state v))) nil form))
                  (sequential? form) (reduce #(or %1 (const-state %2)) nil form)
                  ;; TODO: handle assoc-in, assoc, update-in, update actions
                  :else              nil))]
    (let [{:keys [failed? value]} (const-state edn-expr)]
      (assert (not failed?)
              (format ":state field must be a constant value and not dynamically computed: %s" (pr-str value)))))
  (if (fn? edn-expr)
    (edn-expr (merge {:io    io
                      :state state
                      :input input}
                     (when (not= ::p/nothing output)
                       {:output output})))
    (try
      (sci/eval-string (pr-str edn-expr)
                       {:allow       (-> eval-allows
                                         (into eval-nonterminating)
                                         (into '[io state input output]))
                        :realize-max 100
                        :namespaces  {}
                        :load-fn     (constantly nil)
                        :bindings    (merge eval-bindings
                                            {'io    io
                                             'state state
                                             'input input}
                                            (when (not= ::p/nothing output)
                                              {'output output}))})
      (catch Exception e
        (throw (ex-info "Failed to interpret action" {:code edn-expr} e))))))

(defn clj-eval-action [edn-expr io state input output]
  (try
    (eval `(let [~'io    ~io
                 ~'state ~state
                 ~'input ~input]
             ~(if (= ::p/nothing output)
                edn-expr
                `(let [~'output ~output]
                   ~edn-expr))))
    (catch Exception e
      (throw (ex-info "Failed to interpret action" {:code edn-expr} e)))))

(defrecord Sandboxed []
  p/MachineInterpreter
  (evaluate-expr [_ expr state input output] (eval-action expr io state input output)))

(defrecord Naive []
  p/MachineInterpreter
  (evaluate-expr [_ expr state input output] (clj-eval-action expr io state input output)))
