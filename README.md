# Workflow

Leveraging state machines as a faster way to build web services.

## Problem

Typical web software development needs to handle the following concerns:

 - Implementing new features
 - Reliability of operation & implementation, including upgrades/migrations
 - Observability/Explainability of the system to technical and non-technical people

But today's efforts to address this employ personel & tooling around these
issues, instead of trying to address them directly:

 - Microservices: While a logical way to organize teams, there should be efforts
   to broaden understandability of the system as a whole (for everyone
   involved). Microservices should be easier, faster, & more reliable to build
   that an equivalent monolith.
 - "Micro** Tools: Tools tend to give a limit perspective to solve the wider
   problem. Error tracking, metrics, and log aggregations are a proxy to
   understanding what happened.
 - Processes: Specifications that are independent from code. There is a gap
   understanding how a system work for technical and non-technical people that
   should be narrowed.
 
Nothing is wrong with those above efforts, but this project attempts to imagine
something different.

In short, the goal is make implementations effortless enough to allow people to
better understand problems to solve instead of how to execute effectively.

## Usage

**Note: Nothing here is a stable API. Things are subject to change at this point in time.**

```clojure
(require '[workflow.interpreters :refer [->Sandboxed]])
(require '[workflow.api :as api])
(require '[workflow.memory :as mem])
(defn make []
(let [statem (mem/make-statem-persistence)]
  (wf/effects {:statem      statem
              :execution   (mem/make-execution-persistence statem)
              :scheduler   (mem/make-scheduler)
              :interpreter (->Sandboxed)})))

(def fx (make))
(wf/save-statem fx #:state-machine{:id             "order"
                                  :version        1
                                  :start-at       "create"
                                  :execution-mode "async-throughput"
                                  :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                                  :states         '{"create"    {:always [{:name  "created"
                                                                          :state "cart"}]}
                                                    "cart"      {:actions {"add"    {:name    "added"
                                                                                    :state   "cart"
                                                                                    :context (update-in context [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
                                                                          "remove" {:name    "removed"
                                                                                    :state   "cart"
                                                                                    :context (letfn [(sub [a b]
                                                                                                        (let [a (vec a)
                                                                                                              n (count a)]
                                                                                                          (loop [out (transient [])
                                                                                                                i   0
                                                                                                                b   (frequencies b)]
                                                                                                            (if (= i n)
                                                                                                              (persistent! out)
                                                                                                              (let [ai (a i)]
                                                                                                                (if (pos? (b ai 0))
                                                                                                                  (recur out (inc i) (update b ai dec))
                                                                                                                  (recur (conj! out ai) (inc i) b)))))))]
                                                                                                (update-in context [:order :line-items] (fnil sub []) (repeat (:qty input 1) (:sku input))))}
                                                                          "place"  {:state "submitted"}}}
                                                    "submitted" {:actions {"fraud-approve" {:state "fraud-approved"}
                                                                          "fraud-reject"  {:state "fraud-rejected"}}}

                                                    "fraud-approved" {:always [{:state "released"}]}
                                                    "fraud-rejected" {:actions {"cancel" {:state "canceled"}}}
                                                    "released"       {:always [{:id     "ship"
                                                                                :name   "ship"
                                                                                :invoke {:state-machine ["shipment" 1]
                                                                                        :input         {:order (:id (:order ctx))}
                                                                                        :success       {:state   "ship-finished"
                                                                                                        :context {:delivered (:delivered output)}}
                                                                                        :error         {:state "canceled"}}}]}
                                                    "ship-finished"  {:always [{:name  "fulfilled"
                                                                                :when  (:delivered ctx)
                                                                                :state "shipped"}
                                                                              {:name  "canceled"
                                                                                :state "canceled"}]}
                                                    "shipped"        {:end true}
                                                    "canceled"       {:end true}}})
(wf/save-statem fx #:state-machine{:id             "shipment"
                                  :version        1
                                  :start-at       "created"
                                  :execution-mode "async-throughput"
                                  :context        '{:id        "S1"
                                                    :order     (:order input)
                                                    :delivered false}
                                  :states         '{"created"     {:always [{:name  "fulfilled"
                                                                              :state "outstanding"}]}
                                                    "outstanding" {:always  [{:id     "fetch"
                                                                              :name   "fetched"
                                                                              :invoke {:given (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}})
                                                                                        :if    (<= 200 (:status output) 299)
                                                                                        :then  {:state   "fetched"
                                                                                                :context {:response {:n (:n (:json (:body output)))}}}
                                                                                        :else  {:state "failed"}}}]
                                                                    :actions {"cancel" {:state "canceled"}}}
                                                    "failed"      {:always [{:name     "retry"
                                                                              :state    "outstanding"
                                                                              :wait-for {:seconds 5}}]}

                                                    "canceled" {:end    true
                                                                :return {:delivered false}}

                                                    "fetched" {:always [{:name    "deliver"
                                                                          :state   "delivered"
                                                                          :when    (> 3 (:n (:response ctx)))
                                                                          :context {:response nil
                                                                                    :result   (:n (:response ctx))}}
                                                                        {:name     "retry"
                                                                          :state    "outstanding"
                                                                          :context  {:response nil}
                                                                          :wait-for {:seconds 5}}]}

                                                    "delivered" {:end    true
                                                                  :return {:delivered true}}}})


(p/register-execution-handler fx (wf/create-execution-handler fx))
(def out (wf/start fx "order" nil))
(wf/trigger fx (second out) {::wf/action "add"
                              ::wf/reply? true
                              :sku        "bns12"
                              :qty        1})
(wf/trigger fx (second out) {::wf/action "place"})
(def res (wf/trigger fx (second out) {::wf/action "fraud-approve"
                                      ::wf/reply? true}))
(async/take! res prn)
```

## License

Copyright © 2021 Jeff Hui

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.


## Open Research Areas

Here's some extra things to look into:

 - How to maintain high performance, despite more data being generated.
 - How to minimize the amount of need of escape hatches
 - How to share state machine implementations
