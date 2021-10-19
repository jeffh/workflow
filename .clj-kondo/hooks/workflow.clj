(ns hooks.workflow
  (:require [clj-kondo.hooks-api :as api]))

(defn letlocals [{:keys [node]}]
  (let [[sym & forms] (:children node)
        bindings (butlast forms)]
    {:node (api/list-node
            [(with-meta (api/token-node 'let) (meta sym))
             (api/vector-node
              (vec
               (mapcat
                identity
                (for [b bindings]
                  (or
                   (when (api/list-node? b)
                     (let [[sym var expr] (:children b)]
                       (when (and (api/token-node? sym)
                                  (= "bind" (str sym)))
                         [var expr])))
                   [(api/token-node '_) b])))))
             (last forms)])}))