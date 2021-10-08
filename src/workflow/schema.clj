(ns workflow.schema
  (:require [malli.core :as m]
            [malli.error :as me]))

(def Code any?)

(def State
  [:or string? keyword? integer?])

(def Transition
  [:schema {:registry {::transition [:or
                                     [:map
                                      [:state State]
                                      [:name {:optional true} string?]
                                      [:when {:optional true} Code]
                                      [:wait-for {:optional true}
                                       [:map [:seconds integer?]]]
                                      [:context {:optional true} Code]]
                                     [:map
                                      [:id string?]
                                      [:name {:optional true} string?]
                                      [:when {:optional true} Code]
                                      [:wait-for {:optional true}
                                       [:map [:seconds integer?]]]
                                      [:context {:optional true} Code]
                                      [:invoke
                                       [:or
                                        [:map
                                         [:state-machine [:tuple string? integer?]]
                                         [:input {:optional true} Code]
                                         [:success [:ref ::transition]]
                                         [:error [:ref ::transition]]]
                                        [:map
                                         [:given Code]
                                         [:if Code]
                                         [:then [:ref ::transition]]
                                         [:else [:ref ::transition]]]]]]]}}
   ::transition])

(def StateMachine
  [:map
   [:state-machine/id string?]
   [:state-machine/version integer?]
   [:state-machine/execution-mode string?]
   [:state-machine/context Code]
   [:state-machine/states [:map-of
                           State
                           [:map
                            [:always {:optional true} [:vector Transition]]
                            [:actions {:optional true} [:map-of any? Transition]]
                            [:end {:optional true} boolean?]
                            [:return {:optional true} Code]]]]])

(def Time pos-int?)

(def Execution
  [:map
   [:execution/comment [:maybe string?]]
   [:execution/event-name [:maybe string?]]
   [:execution/event-data [:maybe any?]]
   [:execution/id uuid?]
   [:execution/version integer?]
   [:execution/state-machine-id string?]
   [:execution/state-machine-version integer?]
   [:execution/mode string?]
   [:execution/status [:enum "queued" "running" "failed" "waiting" "paused" "finished" "failed-resumable"]]
   [:execution/state [:maybe State]] ;; nil indicates terminated
   [:execution/memory any?]
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
   [:execution/return-target [:maybe any?]]
   [:execution/wait-for [:maybe any?]]
   [:execution/return-data [:maybe any?]]
   [:execution/end-state [:maybe State]]
   [:execution/dispatch-by-input [:maybe any?]]
   [:execution/dispatch-result [:maybe any?]]])


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

(defn assert-statem [s]
  (assert (valid-statem? s)
          (format "Invalid statem: %s; Input: %s"
                  (pr-str (err-for-statem s))
                  (pr-str s)))
  s)

;;;;;;;;;;

(def ^:private debug true)

(defn debug-assert-execution [e]
  (when debug (assert-execution e)))
(defn debug-assert-statem [s]
  (when debug (assert-statem s)))
