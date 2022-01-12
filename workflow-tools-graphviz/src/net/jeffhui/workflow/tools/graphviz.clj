(ns net.jeffhui.workflow.tools.graphviz
  (:require [clojure.java.shell :as sh]
            [clojure.string :as string]
            [net.jeffhui.workflow.contracts :as contracts]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]))

(defn- str->ident [s]
  (if s
    (pr-str s)
    "\"\""))

(defn- get-edges [statem]
  (let [states (:state-machine/states statem)]
    (apply concat
           (for [[src transition] states
                 action           (:actions transition)
                 :let             [{:keys [id invoke wait-for]} action
                                   wh (:when action)]]
             (remove
              empty?
              (cond invoke
                    (let [invoke-id (cond
                                      (:state-machine invoke)
                                      (str (if (:async? invoke)
                                             "start: "
                                             "run: ")
                                           (pr-str (:state-machine invoke)) "")

                                      (:execution invoke)
                                      (str (if (:async? invoke)
                                             "notify: "
                                             "trigger: ")
                                           (pr-str (:execution invoke)))

                                      (:call invoke)
                                      (str "call: " (pr-str (:call invoke))))]
                      [(format "  edge [color=%s]"
                               (cond
                                 (string? wh) "firebrick3"
                                 wh           "darkgreen"
                                 :else        "black"))
                       (format "  %s -> %s [label=%s]"
                               (str->ident src)
                               (str->ident (or (:state action) invoke-id))
                               (str->ident
                                (str (when wh (pr-str wh)))))
                       (when-not (:state action)
                         (format "  %s [shape=component]" (str->ident invoke-id)))
                       (format "  %s -> %s"
                               (str->ident (or (:state action) invoke-id))
                               (str->ident (:state invoke)))])

                    wait-for
                    [(format "  edge [color=%s]"
                             (cond
                               (string? wh) "darkorange"
                               wh           "darkgreen"
                               :else        "darkgoldenrod1"))
                     (format "  %s -> %s [label=%s,style=dashed]"
                             (str->ident src)
                             (str->ident (or (:state action) src))
                             (str->ident
                              (string/join
                               " & "
                               (remove nil?
                                       [(when wh (pr-str wh))
                                        (cond (:seconds wait-for)
                                              (let [s (:seconds wait-for)]
                                                (format "Wait %ds" s))

                                              (:timestamp wait-for)
                                              (format "Wait until %s" (str (:timestamp wait-for))))]))))]

                    (:if action)
                    [(format "  edge [color]")]

                    :else
                    (let [target (or (:state action) src)]
                      [(format "  edge [color=%s]"
                               (cond
                                 (string? wh) "firebrick3"
                                 wh           "darkgreen"
                                 :else        "black"))
                       (if (= src target)
                         (format "  %s:e -> %s:w [label=%s]"
                                 (str->ident src)
                                 (str->ident target)
                                 (str->ident (str (when wh (pr-str wh)))))
                         (format "  %s -> %s [label=%s]"
                                 (str->ident src)
                                 (str->ident target)
                                 (str->ident (str (when wh (pr-str wh))))))])))))))

(defn statem->dot [statem]
  (let [edges     (get-edges statem)
        terminals (for [[src transition] (:state-machine/states statem)
                        :when            (contains? transition :return)]
                    (format "  %s [shape=octagon];" (pr-str src)))
        title     (str (:state-machine/id statem) "_v" (:state-machine/version statem))]
    (str "digraph " (str->ident title) "{\n"
         "labelloc=t\n"
         "rankdir=\"LR\"\n"
         (format "label=%s\n" (str->ident (str "State Machine: " title)))
         "\n"
         (string/join "\n" edges)
         "\n"
         (string/join "\n" terminals)
         "\n"
         (format "  %s [shape=box];\n" (:state-machine/start-at statem))
         "\n}")))

(defn dot->stream [dot format]
  (let [{:keys [out err]} (sh/sh "dot" (str "-T" (name format)) :in dot :out-enc :bytes)]
    (io/input-stream out)))

(defn stream->file [in-stream filename]
  (with-open [f (io/output-stream (io/file filename))]
    (io/copy in-stream f)))

(defn statem->file [statem format filename]
  (-> statem
      statem->dot
      (dot->stream format)
      (stream->file filename)))

(comment
  (get-edges contracts/order-statem)

  (println (statem->dot contracts/order-statem))

  (spit "test.dot" (statem->dot contracts/order-statem))
  (spit "test.dot" (statem->dot contracts/shipment-statem))

  (statem->file contracts/order-statem :png "output.png")

  )
