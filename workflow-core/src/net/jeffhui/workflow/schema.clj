(ns net.jeffhui.workflow.schema
  (:require [malli.core :as m]
            [malli.error :as me]))

(def Code any?)

(def State
  [:or string? keyword? integer?])

(def Transition
  [:schema {:registry {::std-transition [:map
                                         [:state State]
                                         [:name {:optional true} string?]
                                         [:wait-for {:optional true}
                                          [:map [:seconds integer?]]]
                                         [:context {:optional true} Code]]
                       ::cond-transition [:map
                                          [:state State]
                                          [:name {:optional true} string?]
                                          [:when Code]
                                          [:wait-for {:optional true}
                                           [:map [:seconds integer?]]]
                                          [:context {:optional true} Code]]
                       ::invoke-transition [:map
                                      [:id string?]
                                      [:name {:optional true} string?]
                                      [:when {:optional true} Code]
                                      [:wait-for {:optional true}
                                       [:map [:seconds integer?]]]
                                      [:context {:optional true} Code]
                                      [:invoke
                                       [:or
                                        ;; invoke state machine
                                        [:map
                                         [:state-machine [:tuple string? integer?]]
                                         [:async? {:optional true} boolean?]
                                         [:input {:optional true} Code]
                                         [:success [:ref ::std-transition]]
                                         [:error [:ref ::std-transition]]]
                                        ;; trigger execution
                                        [:map
                                         [:trigger Code]
                                         [:async? {:optional true} boolean?]
                                         [:input {:optional true} Code]
                                         [:success [:ref ::std-transition]]
                                         [:error [:ref ::std-transition]]]
                                        ;; invoke io
                                        [:map
                                         [:given Code]
                                         [:if Code]
                                         [:then [:ref ::std-transition]]
                                         [:else [:ref ::std-transition]]]]]]
                       ::transition [:or
                                     [:ref ::std-transition]
                                     [:ref ::invoke-transition]]}}
   ::transition])

(def TransitionV2
  [:schema {:registry {::std-transition [:map
                                         [:id {:optional true} State]
                                         [:when {:optional true} Code]
                                         [:state {:optional true} State]
                                         [:context {:optional true} Code]
                                         ]
                       ::if-transition [:map
                                        [:id State]
                                        [:state State]
                                        [:when {:optional true} Code]
                                        [:if Code]
                                        [:then [:ref ::std-transition]]
                                        [:else [:ref ::std-transition]]]
                       ::wait-transition [:map
                                          [:id State]
                                          [:state {:optional true} State]
                                          [:context {:optional true} Code]
                                          [:when {:optional true} Code]
                                          [:wait-for
                                           [:or
                                            [:map
                                             [:seconds integer?]
                                             [:state {:optional true} State]
                                             [:context {:optional true} Code]]
                                            [:map
                                             [:timestamp integer?]
                                             [:state {:optional true} State]
                                             [:context {:optional true} Code]]]]]
                       ::invoke-transition [:map
                                            [:id State]
                                            [:state {:optional true} State]
                                            [:context {:optional true} Code]
                                            [:when {:optional true} Code]
                                            [:invoke
                                             [:or
                                              [:map ;; Start Execution (State Machine)
                                               [:state-machine [:tuple Code Code]]
                                               [:async? {:optional true} boolean?]
                                               [:input {:optional true} Code]
                                               [:state {:optional true} State]
                                               [:context {:optional true} Code]]
                                              [:map ;; Trigger Execution
                                               [:execution [:tuple Code Code]]
                                               [:async? {:optional true} boolean?]
                                               [:input {:optional true} Code]
                                               [:state {:optional true} State]
                                               [:context {:optional true} Code]]
                                              [:map ;; IO call
                                               [:call Code]
                                               [:state {:optional true} State]
                                               [:context {:optional true} Code]]]]]
                       ::transition [:or
                                     [:ref ::std-transition]
                                     [:ref ::if-transition]
                                     [:ref ::wait-transition]
                                     [:ref ::invoke-transition]]}}
   ::transition])

(def StateMachine
  [:map
   [:state-machine/id string?]
   [:state-machine/version integer?]
   [:state-machine/execution-mode string?]
   [:state-machine/context {:optional true} Code]
   [:state-machine/io {:optional true} [:map-of any? Code]]
   [:state-machine/states [:map-of
                           State
                           [:or
                            [:map [:return Code]]
                            [:map
                             [:actions [:vector TransitionV2]]]
                            [:map
                             [:always [:vector Transition]]
                             [:actions {:optional true} [:map-of any? Transition]]]
                            [:map
                             [:always {:optional true} [:vector Transition]]
                             [:actions [:map-of any? Transition]]]]]]])

(def Time pos-int?)

(def Execution
  [:map
   [:execution/comment [:maybe string?]]
   [:execution/id uuid?]
   [:execution/version integer?]
   [:execution/state-machine-id string?]
   [:execution/state-machine-version integer?]
   ;; [:execution/io [:maybe [:map-of any? Code]]]
   [:execution/mode string?]
   [:execution/state [:maybe State]] ;; nil indicates terminated
   [:execution/memory any?]
   [:execution/pause-state [:enum "ready" "await-input" "wait" "finished"]]
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
   [:execution/return-to [:maybe any?]]])


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
