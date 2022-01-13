;; VERY EXPERIMENTAL, do not use
(ns net.jeffhui.workflow.builder
  (:refer-clojure :rename {when clj-when})
  (:require [clojure.walk :as walk]))

(defn machine
  ([id] (machine id 1))
  ([id version]
   (assert (string? id))
   (assert (integer? version))
   #:state-machine{:id             id
                   :version        version
                   :execution-mode "async-throughput"
                   :context        nil
                   :states         nil}))

(defmacro starting-ctx [sm ctx]
  `(assoc ~sm :state-machine/context (quote ~ctx)))

(defmacro starts-at
  ([sm state] `(starts-at sm state nil))
  ([sm state context]
   `(merge ~sm #:state-machine{:start-at ~state
                               :context  (quote ~context)})))

(defn add-state [sm name & actions]
  (let [acts (remove :return actions)
        return (first (filter :return actions))]
    (-> sm
        (assoc-in [:state-machine/states name]
                  (or return {:actions (vec actions)}))
        (cond-> (empty? (:state-machine/states sm)) (assoc :state-machine/start-at name)))))

(defn make-state [name & actions]
  (let [acts (remove :return actions)
        return (first (filter :return actions))]
    {name (or return {:actions (vec actions)})}))

(defmacro make-states [name-action-pairs]
  `(merge ~@(map (fn [[n & actions]] `(make-state ~n ~@actions))
                 name-action-pairs)))

(defmacro always
  [id & actions]
  `(merge {:id ~id}
          ~@actions))

(defmacro when
  [clause & actions]
  `(merge {:id   ~(or (:id (meta clause)) (str clause))
           :when (quote ~clause)}
          ~@actions))

(defmacro goto-state [state] `{:state (quote ~state)})
(defmacro set-ctx [ctx] `{:context (quote ~ctx)})
(defmacro assoc-ctx ([key f & args] {:context `(quote ~(apply list 'assoc 'ctx key f args))}))
(defmacro update-ctx ([key f & args] {:context `(quote ~(apply list 'update 'ctx key f args))}))

(defmacro invoke-async-statem
  ([state-machine-id input action & actions]
   `{:invoke (merge {:state-machine (quote ~state-machine-id)
                     :input         (quote ~input)
                     :async?        true}
                    ~action
                    ~@actions)}))

(defmacro invoke-sync-statem
  ([state-machine-id input action & actions]
   `{:invoke (merge {:state-machine (quote ~state-machine-id)
                     :input         (quote ~input)
                     :async?        false}
                    ~action
                    ~@actions)}))

(defmacro invoke-async-execution [execution-id input action & actions]
  `{:invoke (merge {:execution (quote ~execution-id)
                    :input         (quote ~input)
                    :async?        true}
                   ~action
                   ~@actions)})

(defmacro invoke-sync-execution [execution-id input action & actions]
  `{:invoke (merge {:execution (quote ~execution-id)
                    :input         (quote ~input)
                    :async?        false}
                   ~action
                   ~@actions)})

(defmacro return [value] `{:return (quote ~value)})


(comment
  (-> (machine "wishlist")
      (state "idle"
             (when "add"
               (update-ctx :items (fnil conj #{}) (:sku input)))
             (when "removed"
               (update-ctx :items disj (:sku 'input)))
             (when "create-order"
               (invoke-async-statem ["prepare-cart" 1]
                                    nil
                                    (goto-state "idle")
                                    (set-ctx {:skus (:items ctx)})))))

  (-> (machine "prepare-cart")
      (starting-ctx {:requirements (set (:skus input))
                     :left         (set (:skus input))})
      (state "state"
             (when "create-cart"
               (invoke-async-statem ["order" 1] nil
                                    (goto-state "started")
                                    (update-ctx :invoke-response {:order-eid (:execution/id (:value output))
                                                                  :ok        (:ok output)}))))
      (state "started"
             (when ^{:id "started-success"} (:ok (:invoke-response input))
               (goto-state "adding"))
             (when "started-failed"
               (goto-state "failed")))
      (state "adding"
             (when ^{:id "adding"} (seq (:left ctx))
               (invoke-sync-execution [(:order-eid ctx) "add"]
                                      {:sku (first (:left ctx))
                                       :qty 1}
                                      (goto-state "added")
                                      (assoc-ctx :last-output output))))
      (state "added"
             (when ^{:id "decide"} (:ok (:last-output ctx))
               (goto-state "complete")
               (update-ctx :left disj (first (:left ctx))))
             (always "fail" (goto-state "failed")))
      (state "complete" (return {:ok true}))
      (state "failed" (return {:ok false}))
      )

  (make-states
   {"state" [(when "create-cart"
               (invoke-async-statem ["order" 1] nil
                                    (goto-state "started")
                                    (update-ctx :invoke-response {:order-eid (:execution/id (:value output))
                                                                  :ok        (:ok output)})))]
    "started" [(when ^{:id "started-success"} (:ok (:invoke-response input))
                 (goto-state "adding"))
               (when "started-failed"
                 (goto-state "failed"))]
    "adding" [(when ^{:id "adding"} (seq (:left ctx))
                (invoke-sync-execution [(:order-eid ctx) "add"]
                                       {:sku (first (:left ctx))
                                        :qty 1}
                                       (goto-state "added")
                                       (assoc-ctx :last-output output)))]
    "added" [(when ^{:id "decide"} (:ok (:last-output ctx))
               (goto-state "complete")
               (update-ctx :left disj (first (:left ctx))))
             (always "fail" (goto-state "failed"))]
    "complete" [(return {:ok true})]
    "failed" [(return {:ok false})]})

  )

