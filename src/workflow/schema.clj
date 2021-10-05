(ns workflow.schema
  (:require [malli.core :as m]
            [malli.error :as me]))

(def Code any?)

(def State
  [:or string? keyword? integer?])

(def Transition
  [:schema {:registry {::transition [:map
                                     [:name {:optional true} string?]
                                     [:id {:optional true} string?]
                                     [:state {:optional true} State]
                                     [:when {:optional true} Code]
                                     [:wait-for {:optional true}
                                      [:map [:seconds integer?]]]
                                     [:context {:optional true} Code]
                                     [:invoke {:optional true}
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
                                        [:else [:ref ::transition]]]]]]}}
   ::transition])

(def StateMachine
  [:map
   [:state-machine/id string?]
   [:state-machine/version integer?]
   [:state-machine/execution-mode string?]
   [:state-machine/context Code]
   [:state-machine/states
    [:map-of
     State
     [:map
      [:always {:optional true} [:vector Transition]]
      [:actions {:optional true} [:map-of any? Transition]]
      [:end {:optional true} boolean?]
      [:return {:optional true} Code]]]]])


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
   [:execution/state State]
   [:execution/memory any?]
   [:execution/input any?]
   [:execution/enqueued-at [:maybe inst?]]
   [:execution/started-at [:maybe inst?]]
   [:execution/finished-at [:maybe inst?]]
   [:execution/failed-at [:maybe inst?]]
   [:execution/step-started-at [:maybe inst?]]
   [:execution/step-ended-at [:maybe inst?]]
   [:execution/user-started-at [:maybe inst?]]
   [:execution/user-ended-at [:maybe inst?]]
   [:execution/error [:maybe any?]]
   [:execution/return-target [:maybe any?]]
   [:execution/wait-for [:maybe any?]]
   [:execution/return-data [:maybe any?]]
   [:execution/end-state [:maybe State]]])


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

(def *debug* true)

(defn debug-assert-execution [e]
  (when *debug* (assert-execution e)))
(defn debug-assert-statem [s]
  (when *debug* (assert-statem s)))
