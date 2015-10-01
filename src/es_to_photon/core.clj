(ns es-to-photon.core
  (:require [clj-http.client :as client]
            [clojure.data.json :as json]
            [clj-time.coerce :as c]
            [clj-time.format :as f]
            [muon-clojure.client :refer :all]
            [snippets-generic :as sg]))

(def headers
  {:headers {:Accept "application/vnd.eventstore.atom+json"}})

(defn json->clj [j]
  (json/read-str j :key-fn keyword))

(defn eventstore-dump
  ([url]
   (let [res (client/get url headers)
         body (json->clj (:body res))
         entries (map #(->
                        (json->clj
                         (:body (client/get (:id %) headers)))
                        :content :data)
                      (:entries body))
         links (:links body)
         next-link (:uri (first (filter
                                 #(= (:relation %) "previous")
                                 links)))]
     (if (empty? entries)
       []
       (concat entries (lazy-seq (eventstore-dump next-link))))))
  ([ip port stream]
   (let [url (str "http://" ip ":" port "/streams/" stream
                  "/0/forward/20")]
     (eventstore-dump url))))

(def custom-formatter (f/formatter "EEE MMM dd YYYY HH:mm:ss 'GMT'Z"))

(defn date-parsed [st-date]
  (c/to-long
   (f/parse
    custom-formatter
    (clojure.string/join
     " "
     (drop-last (clojure.string/split st-date #"\s"))))))

(defn es->ph [event stream-name]
  (let [t (date-parsed (:server_timestamp event))]
    {:server-timestamp t
     :stream-name stream-name
     :service-id (:session_id event)
     :local-id (:id event)
     :payload event}))

(defn transfer! []
  (let [props (sg/load-props "config.properties")]
    (let [events (eventstore-dump
                  (:eventstore.ip props)
                  (read-string (str (:eventstore.port props)))
                  (:eventstore.stream props))
          valid (filter #(not (or (nil? (:server_timestamp %))
                                  (= "" (:server_timestamp %))))
                        events)
          ph-events (map #(es->ph % (:photon.stream props)) valid)
          m (muon-client (:amqp.url props) "es-to-photon" "client")]
      (dorun (map #(do
                     (clojure.pprint/pprint %)
                     (loop [res nil]
                       (if (nil? res)
                         (let [new-res (with-muon m
                                         (post-event
                                          "muon://photon/events" %))]
                           #_(println new-res)
                           (recur new-res)))))
                  ph-events)))))

