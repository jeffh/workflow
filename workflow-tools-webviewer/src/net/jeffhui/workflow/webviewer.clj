(ns net.jeffhui.workflow.webviewer
  (:require [net.jeffhui.workflow.webviewer.handlers :as handlers]
            [net.jeffhui.workflow.protocol :as protocol]
            [reitit.ring :as rring]
            [aleph.http :as http]
            [net.jeffhui.workflow.api :as api]
            [net.jeffhui.workflow.memory :as mem]))

(defn create-ring-handler [fx options]
  (rring/ring-handler (handlers/create-router fx options)
                      (rring/redirect-trailing-slash-handler {:method :strip})))

(defrecord HttpServer [fx options handler server]
  protocol/Connection
  (open* [this]
    (when server (.close ^java.io.Closeable server))
    (assoc this :server (http/start-server handler options)))
  (close* [this]
    (when server
      (.close ^java.io.Closeable server))
    (dissoc this :server)))

(defn create-http-server
  ([fx server-options] (create-http-server fx server-options nil))
  ([fx server-options handler-options] (->HttpServer fx
                                                     server-options
                                                     (if (fn? handler-options)
                                                       (handler-options fx)
                                                       (create-ring-handler fx handler-options))
                                                     nil)))

(comment
  (do
    (require '[net.jeffhui.workflow.interpreters :refer [->Sandboxed ->Naive]] :reload)
    (defn make []
      (api/effects {:statem      (mem/make-statem-persistence)
                    :execution   (mem/make-execution-persistence)
                    :scheduler   (mem/make-scheduler)
                    :interpreter (->Sandboxed)}))
    (def fx (api/open (make)))
    (def server (api/open (create-http-server fx {:port 8881}))))

  (api/list-statem fx {:limit 10})
  (api/list-executions fx {:limit 10})

  (:server server)

  (api/close server)

  ((create-ring-handler fx {}) {:request-method :get
                                :uri "/"
                                :scheme :http
                                :server-port 8888
                                :server-name "localhost"
                                :body ""})


  )
