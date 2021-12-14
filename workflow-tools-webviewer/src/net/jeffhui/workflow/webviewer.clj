(ns net.jeffhui.workflow.webviewer
  (:require [net.jeffhui.workflow.webviewer.handlers :as handlers]
            [net.jeffhui.workflow.protocol :as protocol]
            [reitit.ring :as rring]
            [aleph.http :as http]))

(defn create-ring-handler [fx options]
  (rring/ring-handler (handlers/create-router fx options)
                      (rring/redirect-trailing-slash-handler {:method :strip})))

(defrecord HttpServer [fx options handler server]
  protocol/Connection
  (open* [this]
         (when server (.close ^java.io.Closable server))
         (assoc this :server (http/start-server handler options)))
  (close* [_]
          (when server
            (.close ^java.io.Closable server))))

(defn create-http-server
  ([server-options] (create-http-server server-options nil))
  ([server-options handler-options] (->HttpServer fx
                                                  server-options (if (fn? handler-options)
                                                                   (handler-options fx)
                                                                   (create-ring-handler fx handler-options))
                                                  nil)))