(ns net.jeffhui.workflow.webviewer.handlers
  (:require [net.jeffhui.workflow.api :as api]
            [hiccup2.core :refer [html]]
            [jsonista.core :as json]
            [clojure.pprint :as pprint]
            [clojure.string :as string]
            [reitit.core :as r]
            [reitit.ring :as rring]))

(def accept->keyword
  {"application/json" :json
   "application/edn" :edn
   "text/html" :html
   "text/plain" :text})

(def default-nav
  [["Machines" ::list-machines]
   ["Executions" ::list-executions]])

(defn- get-accept [req]
  (let [accepts (string/lower-case (get (:headers req) "accept"))]
    (accept->keyword accepts :html)))

(defn default-layout [options {::r/keys [router match] :as req} {:keys [status data to-html]}]
  (condp = (get-accept req)
    :json {:status (int status)
           :headers {"Content-Type" "application/json"}
           :body (json/write-value-as-string data)}

    :edn {:status (int status)
          :headers {"Content-Type" "application/edn"}
          :body (pr-str data)}

    :text {:status (int status)
           :headers {"Content-Type" "text/plain"}
           :body (with-out-str
                   (if (map? data)
                     (doseq [[f v] data]
                       (println (str f ":"))
                       (if (coll? v)
                         (pprint/print-table v)
                         (pprint/pprint v)))
                     (pprint/pprint data)))}

    (let [nav (or (:nav options)
                  [["Machines" ::list-machines]
                   ["Executions" ::list-executions]])]
      {:status (int status)
       :body (html
              [:html {:lang "en"}
               [:head
                [:title "Workflow Webviewer"]
                [:link {:href (r/match-by-name router ::asset)}]]
               [:body
                [:header
                 [:h1 "Workflow Webviewer"]
                 [:ul#main-nav
                  (for [[text id] nav]
                    [:li [:a.block
                          {:id (str id)
                           :href (r/match-by-name router id)
                           :class (when (= id (:name match))
                                    "selected")}
                          text]])]]
                [:main
                 (to-html data)]]])})))

(defn- layout [options req response-data]
  (let [layer (or (:layout options)
                  default-layout)]
    (layer (dissoc options :layout) req response-data)))

(defn redirect-to [id]
  (fn handler [{::r/keys [router]}]
    {:status 302
     :headers {"Location" (r/match-by-name router id)}}))

(defn list-state-machines [fx options]
  (fn handler [req]
    (layout options req
            {:status 200
             :data {:state-machines (api/list-statem fx {:limit 100
                                                         :fields [:state-machine/id
                                                                  :state-machine/version]})}
             :to-html (fn [{:keys [state-machines]}]
                        [:div
                         [:nav.breadcrumbs
                          [:ol [:li "Machines"]]]
                         [:h1 "State Machines"]
                         (if (seq state-machines)
                           (for [m state-machines]
                             [:section
                              [:h2 (str (:state-machine/id m))]])
                           [:p "No state machines"])])})))

(defn show-state-machine [req])
(defn list-executions [fx options]
  (fn handler [req]
    (layout options req
            {:status 200
             :data {:executions (api/list-executions fx {:limit 100})}
             :to-html (fn [{:keys [executions]}]
                        [:div
                         [:nav.breadcrumbs
                          [:ol [:li "Machines"]]]])})))
(defn show-execution [req])


(defn create-router [fx options]
  (rring/router
   [["/machines" {:name ::list-machines
                  :get (list-state-machines fx options)}]
    ["/machines/:id" {:name ::show-machine
                      :get show-state-machine}]
    ["/machines/:id/:version" {:name ::show-machine
                               :get show-state-machine}]
    ["/executions" {:name ::list-executions
                    :get list-executions}]
    ["/executions/:id" {:name ::show-execution
                        :get show-execution}]
    ["/executions/:id/:version" {:name ::show-execution
                                 :get show-execution}]
    ["/assets/*" {:name ::asset
                  :get (rring/create-resource-handler)}]
    ["/" {:name ::home
          :get (redirect-to ::list-machines)}]]))
