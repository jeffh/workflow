(ns net.jeffhui.workflow.io.http
  (:require [net.jeffhui.workflow.protocol :as protocol]
            [clojure.string :as string]
            [jsonista.core :as json])
  (:import java.net.http.HttpClient$Version
           java.net.http.HttpClient$Redirect
           java.net.http.HttpClient
           java.net.http.HttpRequest
           java.net.http.HttpRequest$BodyPublishers
           java.net.http.HttpResponse
           java.net.http.HttpResponse$BodyHandlers
           java.util.concurrent.CompletableFuture
		   java.util.function.Function
           java.time.Duration
           java.net.URLEncoder))

(defn- then [^CompletableFuture cf f]
  (.thenApply cf (reify Function (apply [_ t] (f t)))))

(def ^:private http-client
  (delay (-> (HttpClient/newBuilder)
             (.followRedirects HttpClient$Redirect/NORMAL)
             (.connectTimeout (Duration/ofSeconds (long 15)))
             (.build))))


(defn- form-encode [m]
  (string/join "&"
               (map (fn [[k v]]
                      (str (URLEncoder/encode (str k) "UTF-8") "="
                           (URLEncoder/encode (str v) "UTF-8")))
                    m)))

(defn- ->request [method url {:keys [timeout]
                              :or {timeout 15000}
                              :as options}]
  (let [b (doto (HttpRequest/newBuilder)
            (.uri (java.net.URI. (str url)))
            (.method (string/upper-case (name method))
                     (or
                      (when-let [b (:json-body options)]
                        (HttpRequest$BodyPublishers/ofString (json/write-value-as-string b)))
                      (when-let [b (:edn-body options)]
                        (HttpRequest$BodyPublishers/ofString (pr-str b)))
                      (when-let [b (:body options)]
                        (cond
                          (map? b) (HttpRequest$BodyPublishers/ofString (form-encode b))
                          (bytes? b) (HttpRequest$BodyPublishers/ofByteArrays b)
                          (string? b) (HttpRequest$BodyPublishers/ofString b)
                          (nil? b) nil ;; fallthrough
                          :else (throw (IllegalArgumentException. "Unsupported body"))))
                      (HttpRequest$BodyPublishers/noBody)))
            (.timeout (java.time.Duration/ofMillis (long timeout))))]
    (doseq [[k v] (:headers options)]
      (if (coll? v)
        (doseq [v' v] (.addHeader b (str k) (str v')))
        (.setHeader b (str k) (str v))))
    (.build b)))

(defn- simple-response->map [^HttpResponse res]
  {:version (condp = (.version res)
              HttpClient$Version/HTTP_1_1 [1 1]
              HttpClient$Version/HTTP_2 [2 0])
   :url     (str (.uri res))
   :status  (.statusCode res)
   :body    (.body res)
   :headers (into {} (map (fn [[k v]] [k (into [] v)])) (.map (.headers res)))})

(defn- response->map [^HttpResponse res]
  (with-meta
    (simple-response->map res)
    (let [req (.request res)]
      {:request {:method  (.method req)
                 :url     (str (.uri req))
                 :headers (into {} (map (fn [[k v]] [k (into [] v)])) (.map (.headers req)))}
       :responses (let [out (transient [])]
                    (loop [maybe-res (.previousResponse res)]
                      (if (.isEmpty maybe-res)
                        (persistent! out)
                        (do
                          (conj! out (simple-response->map res))
                          (recur (.previousResponse (.get res)))))))})))

(defmethod protocol/io "http.request" http-request [_ method url options]
  (let [req (->request method url options)
        res (-> (.sendAsync @http-client req
                            (let [b (:as options)]
                              (condp = b
                                :bytes (HttpResponse$BodyHandlers/ofByteArray)
                                (HttpResponse$BodyHandlers/ofString))))
                (then response->map))]
    (.join res)))

(defmethod protocol/io "http.request.json" http-request-json [_ method url options]
  (let [req (->request method url options)
        res (-> (.sendAsync @http-client req
                            (let [b (:as options)]
                              (condp = b
                                :bytes (HttpResponse$BodyHandlers/ofByteArray)
                                (HttpResponse$BodyHandlers/ofString))))
                (then response->map)
                (then #(update % :body json/read-value json/keyword-keys-object-mapper)))]
    (.join res)))
