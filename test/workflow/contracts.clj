(ns workflow.contracts
  (:require [clojure.test :refer [testing is]]
            [workflow.api :as api]
            [workflow.memory :as mem]
            [workflow.protocol :as protocol]))

(declare order-statem shipment-statem)
(defn statem-persistence [doc-name creator]
  (testing doc-name
    (testing "conforms to state machine persistence"
    (let [persistence (api/open (creator))]
        (try
          (testing "[happy path]"
            (testing "save-statem returns a future that saves the state machine"
              (let [r (api/save-statem persistence order-statem)]
                (is (future? r) "save-statem should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-statem should succeed")
                (is (= order-statem (:entity (deref r 1000 :timeout))))))

            (testing "fetch-statem returns previously stored state machines"
              (is (= order-statem (api/fetch-statem persistence
                                                    (:state-machine/id order-statem)
                                                    (:state-machine/version order-statem)))
                  "persistence should return what was stored")
              (is (= order-statem (api/fetch-statem persistence (:state-machine/id order-statem) :latest))
                  "persistence should return latest state machine version"))

            (testing "fetching :latest returns the latest state machine version"
              (let [v2 (assoc order-statem :state-machine/version 2)]
                (is (:ok (deref (api/save-statem persistence v2)
                                1000 :timeout))
                    "given version 2 of a state machine is saved")
                (is (= v2 (api/fetch-statem persistence (:state-machine/id order-statem) :latest))
                    "using version = :latest should return v2")))

            (testing "checking that v1 of statem still exists after saving v2"
              (is (= order-statem (api/fetch-statem persistence
                                                    (:state-machine/id order-statem)
                                                    (:state-machine/version order-statem)))
                  "persistence should return what was stored")))

          (testing "[exceptional cases]"
            (testing "ignores duplicate state machine version"
              (let [r (api/save-statem persistence (assoc shipment-statem
                                                          :state-machine/id (:state-machine/id order-statem)
                                                          :state-machine/version (:state-machine/version order-statem)))]
                (is (not= :timeout (deref r 1000 :timeout))
                    "save may succeed or fail"))

              (is (= order-statem (api/fetch-statem persistence (:state-machine/id order-statem) (:state-machine/version order-statem)))
                  "the original state machine remains unchanged")))

          (testing "[input validation cases]"
            (testing "persistence ignores saves without an id and version"
              (letfn [(error-save [statem]
                        (not (:ok (deref (api/save-statem persistence statem) 1000 :timeout))))]
                (is (error-save (dissoc order-statem :state-machine/id))
                    "errors without state machine id")
                (is (error-save (dissoc order-statem :state-machine/version))
                    "errors without state machine version")
                (is (error-save (dissoc order-statem :state-machine/id :state-machine/version))
                    "errors without state machine id and version")
                (testing "errors when :state-machine/version is not an integer"
                  (is (error-save (assoc order-statem :state-machine/version "1")))
                  (is (error-save (assoc order-statem :state-machine/version 1.2)))
                  (is (error-save (assoc order-statem :state-machine/version 1M)))))))

          (finally
            (api/close persistence)))))))


(declare execution-started)
(defn execution-persistence [doc-name creator]
  (testing doc-name
    (testing "conforms to execution persistence"
      (let [persistence (api/open (creator))]
        (try
          (testing "[happy path]"
            (testing "save-execution returns a future that saves the execution"
              (let [r (protocol/save-execution persistence execution-started)]
                (is (future? r) "save-execution should return a future")
                (is (:ok (deref r 1000 :timeout)) "save-execution should succeed")
                (is (= execution-started (:entity (deref r 1000 :timeout))))))
            (testing "fetch-execution returns the execution of the specific version"
              (is (= execution-started (api/fetch-execution persistence
                                                            (:execution/id execution-started)
                                                            (:execution/version execution-started)))
                  "persistence should return what was stored")))
          (finally
            (api/close persistence)))))))

(def ^:private execution-started
  #:execution{:comment               "Enqueued for execution"
              :event-name            nil
              :event-data            nil
              :id                    #uuid "3DB0A06E-7F9C-445A-B327-CE9B4EEC9E53"
              :version               1
              :state-machine-id      "order"
              :state-machine-version 1
              :mode                  "async-throughput"
              :status                "queued"
              :state                 "create"
              :memory                {:order {:id "R4523"}}
              :input                 nil
              :enqueued-at           #inst "2021-10-04T09:06:32.203-00:00"
              :started-at            nil
              :finished-at           nil
              :failed-at             nil
              :step-started-at       nil
              :step-ended-at         nil
              :user-started-at       nil
              :user-ended-at         nil
              :error                 nil
              :return-target         nil
              :wait-for              nil
              :return-data           nil
              :end-state             nil})

(def ^:private order-statem
  #:state-machine{:id             "order"
                  :version        1
                  :start-at       "create"
                  :execution-mode "async-throughput"
                  :context        '{:order {:id (str "R" (+ 1000 (rand-int 10000)))}}
                  :states         '{"create"         {:always [{:name  "created"
                                                                :state "cart"}]}
                                    "cart"           {:actions {"add"    {:name    "added"
                                                                          :state   "cart"
                                                                          :context (update-in state [:order :line-items] (fnil into []) (repeat (:qty input 1) (:sku input)))}
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
                                                                                     (update-in state [:order :line-items] (fnil sub []) (repeat (:qty input 1) (:sku input))))}
                                                                "place"  {:state "submitted"}}}
                                    "submitted"      {:actions {"fraud-approve" {:state "fraud-approved"}
                                                                "fraud-reject"  {:state "fraud-rejected"}}}

                                    "fraud-approved" {:always [{:state "released"}]}
                                    "fraud-rejected" {:actions {"cancel" {:state "canceled"}}}
                                    "released"       {:always [{:id     "ship"
                                                                :name   "ship"
                                                                :invoke {:state-machine ["shipment" 1]
                                                                         :input         {:order (:id (:order state))}
                                                                         :success       {:state   "ship-finished"
                                                                                         :context {:delivered (:delivered output)}}
                                                                         :error         {:state "canceled"}}}]}
                                    "ship-finished"  {:always [{:name  "fulfilled"
                                                                :when  (:delivered state)
                                                                :state "shipped"}
                                                               {:name  "canceled"
                                                                :state "canceled"}]}
                                    "shipped"        {:end true}
                                    "canceled"       {:end true}}})

(def ^:private shipment-statem
  #:state-machine{:id             "shipment"
                  :version        1
                  :start-at       "created"
                  :execution-mode "async-throughput"
                  :context        '{:id        "S1"
                                    :order     (:order input)
                                    :delivered false}
                  :states         '{"created"     {:always [{:name  "fulfilled"
                                                             :state "outstanding"}]}
                                    "outstanding" {:always  [{:name   "fetched"
                                                              :invoke {:given (io "http.request.json" :post "https://httpbin.org/anything" {:json-body {"n" (rand-int 10)}})
                                                                       :if    (<= 200 (:status output) 299)
                                                                       :then  {:state   "fetched"
                                                                               :context {:response {:n (:n (:json (:body output)))}}}
                                                                       :else
                                                                       {:state "failed"}}}]
                                                   :actions {"cancel" {:state "canceled"}}}
                                    "failed"      {:always [{:name     "retry"
                                                             :state    "outstanding"
                                                             :wait-for {:seconds 5}}]}

                                    "canceled" {:end    true
                                                :return {:delivered false}}

                                    "fetched" {:always [{:name    "deliver"
                                                         :state   "delivered"
                                                         :when    (> 3 (:n (:response state)))
                                                         :context {:response nil
                                                                   :result   (:n (:response state))}}
                                                        {:name     "retry"
                                                         :state    "outstanding"
                                                         :context  {:response nil}
                                                         :wait-for {:seconds 5}}]}

                                    "delivered" {:end    true
                                                 :return {:delivered true}}}})

