(ns net.jeffhui.workflow.schema
  (:require [malli.core :as m]
            [malli.error :as me]
            [clojure.set :as set]))

(def Code any?)

(def StateId [:or string? keyword? integer?])

(def Transition
  [:schema {:registry {::std-transition [:and
                                         [:or
                                          [:map [:state StateId]]
                                          [:map
                                           [:ctx Code] ;; :context is deprecated, use :ctx instead
                                           [:context {:optional true} Code]]]
                                         [:map
                                          [:id StateId]
                                          [:when {:optional true} Code]
                                          [:state {:optional true} StateId]
                                          [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                          [:context {:optional true} Code]]]
                       ::if-transition [:map
                                        [:id StateId]
                                        [:state {:optional true} StateId]
                                        [:when {:optional true} Code]
                                        [:if Code]
                                        [:then [:ref ::std-transition]]
                                        [:else [:ref ::std-transition]]]
                       ::wait-transition [:map
                                          [:id StateId]
                                          [:state {:optional true} StateId]
                                          [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                          [:context {:optional true} Code]
                                          [:when {:optional true} Code]
                                          [:wait-for
                                           [:or
                                            [:map
                                             [:seconds integer?]
                                             [:state {:optional true} StateId]
                                             [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                             [:context {:optional true} Code]]
                                            [:map
                                             [:timestamp integer?]
                                             [:state {:optional true} StateId]
                                             [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                             [:context {:optional true} Code]]]]]
                       ::invoke-transition [:map
                                            [:id StateId]
                                            [:state {:optional true} StateId]
                                            [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                            [:context {:optional true} Code]
                                            [:when {:optional true} Code]
                                            [:invoke
                                             [:or
                                              [:map ;; Start Execution (StateId Machine)
                                               [:state-machine [:tuple Code Code]]
                                               [:async? {:optional true} boolean?]
                                               [:input {:optional true} Code]
                                               [:state StateId]
                                               [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                               [:context {:optional true} Code]]
                                              [:map ;; Trigger Execution
                                               [:execution [:tuple Code Code]]
                                               [:async? {:optional true} boolean?]
                                               [:input {:optional true} Code]
                                               [:state StateId]
                                               [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                               [:context {:optional true} Code]]
                                              [:map ;; IO call
                                               [:call Code]
                                               [:state StateId]
                                               [:ctx {:optional true} Code] ;; :context is deprecated, use :ctx instead
                                               [:context {:optional true} Code]]]]]
                       ::transition [:or
                                     [:ref ::std-transition]
                                     [:ref ::if-transition]
                                     [:ref ::wait-transition]
                                     [:ref ::invoke-transition]]}}
   ::transition])

(def ^:private state-id?
  (m/validator StateId))

(defn- state-has-unique-transition-ids [actions]
  (let [values (mapv :id actions)]
    (= (count values) (count (distinct values)))))

(defn- state-has-unique-whens [actions]
  (let [values (into []
                     (comp
                      (map :when)
                      (remove nil?))
                     actions)]
    (= (count values) (count (distinct values)))))

(defn- unique-of [xf]
  (fn unique [actions]
    (let [values (into [] xf actions)]
      (= (count values) (count (distinct values))))))

(defn- multiple-reference-error [key]
  (fn [{:keys [value] :as error} _]
    (let [f (into {}
                  (comp
                   (filter (fn [[k v]] (< 1 v)))
                   (map (fn [[k v]] [k "has been defined multiple times, but should be unique"])))
                  (frequencies (map key value)))]
      {key f})))

(def State
  [:or
   [:map [:return Code]]
   [:map [:actions [:and
                    [:vector Transition]
                    [:fn {:error/fn (multiple-reference-error :id)} (unique-of (map :id))]
                    [:fn {:error/fn (multiple-reference-error :when)} (unique-of (comp (map :when) (remove nil?)))]]]]])

#_
(me/humanize
 (m/explain State {:actions [{:id "a"
                              :when "a"
                              :state "state1"}
                             {:id "a"
                              :when "a"
                              :state "state1"}]}))

(def StateMachine
  [:map
   [:state-machine/id string?]
   [:state-machine/version integer?]
   [:state-machine/execution-mode string?]
   [:state-machine/ctx {:optional true} Code]
   [:state-machine/context {:optional true} Code]  ;; :context is deprecated, use :ctx instead
   [:state-machine/io {:optional true} [:map-of any? Code]]
   [:state-machine/states [:map-of StateId State]]])

(def Time pos-int?)

(def CompleteRef [:or string? uuid?])
(def EffectOps [:enum
                :execution/start
                :execution/step
                :execution/return
                :invoke/io
                :sleep/seconds
                :sleep/timestamp])

(def Execution
  [:map
   [:execution/comment [:maybe string?]]
   [:execution/id uuid?]
   [:execution/version integer?]
   [:execution/state-machine-id string?]
   [:execution/state-machine-version integer?]
   [:execution/io {:optional true} [:maybe [:map-of any? Code]]]
   [:execution/mode string?]
   [:execution/state [:maybe StateId]] ;; nil indicates terminated
   [:execution/ctx any?]
   [:execution/memory {:optional true} any?] ;; Deprecated: legacy key, now :execution/ctx
   [:execution/pause-state [:enum "ready" "await-input" "wait-fx" "finished"]]
   [:execution/pause-memory any?]
   [:execution/input any?]
   [:execution/enqueued-at [:maybe Time]]
   [:execution/started-at [:maybe Time]]
   [:execution/finished-at [:maybe Time]]
   [:execution/failed-at [:maybe Time]]
   [:execution/step-started-at [:maybe Time]]
   [:execution/step-ended-at [:maybe Time]]
   [:execution/user-started-at [:maybe Time]]
   [:execution/user-ended-at [:maybe Time]]
   [:execution/error [:maybe any?]]
   [:execution/return-to [:maybe any?]]
   [:execution/pending-effects [:maybe [:vector [:map
                                                 [:op EffectOps]
                                                 [:args {:optional true} any?]
                                                 [:complete-ref {:optional true} CompleteRef]]]]]
   [:execution/completed-effects [:maybe [:vector [:map
                                                   [:net.jeffhui.workflow.api/resume
                                                    [:map
                                                     [:id CompleteRef]
                                                     [:op EffectOps]
                                                     [:return
                                                      [:or ;; can be more keys depending on the effect's return value
                                                       [:map
                                                        [:ok [:enum false]]
                                                        [:error any?]]
                                                       [:map
                                                        [:ok [:enum true]]]]]]]]]]]])


;;; "Soft" interface boundary: incase we want to replace the validation library

(def valid-execution? (m/validator Execution))
(def valid-statem? (m/validator StateMachine))

(def ^:private statem-explainer (m/explainer StateMachine))
(defn err-for-execution [e]
  (-> Execution
      (m/explain e)
      (me/humanize)))
(defn err-for-statem [e]
  (-> StateMachine
      (m/explain e)
      (me/humanize)))

(defn assert-execution [e]
  (assert (valid-execution? e)
          (format "Invalid execution: %s; Input: %s"
                  (pr-str (err-for-execution e))
                  (pr-str e)))
  e)

(defn assert-statem
  ([s]
   (assert (valid-statem? s)
           (format "Invalid statem: %s; Input: %s"
                   (pr-str (err-for-statem s))
                   (pr-str s)))
   s)
  ([s md]
   (assert (valid-statem? s)
           (format "Invalid statem: %s; Input: %s; %s"
                   (pr-str (err-for-statem s))
                   (pr-str s)
                   (if md
                     (pr-str md)
                     "")))
   s))

;;;;;;;;;;

(def ^:private debug true)

(defn debug-assert-execution [e]
  (when debug (assert-execution e)))
(defn debug-assert-statem
  ([s] (when debug (assert-statem s)))
  ([s md] (when debug (assert-statem s md))))


(declare edn?)
(defn assert-edn [v msg]
  (when-not (edn? v)
    (throw (AssertionError. msg)))
  v)

(defn- edn? [f]
  (cond
    (or (string? f)
        (number? f)
        (seq? f)
        (keyword? f)
        (nil? f)
        (symbol? f)
        (inst? f)
        (uuid? f)
        (boolean? f))
    true

    (tagged-literal? f)
    (edn? (:form f))

    (map? f)
    (and (every? edn? (keys f))
         (every? edn? (vals f)))

    (or (seq? f)
        (list? f)
        (vector? f)
        (set? f))
    (every? edn? f)

    :else false))

(defn assert-errorable [m msg]
  (when-not (contains? m :error)
    (throw (AssertionError. (str msg))))
  m)
