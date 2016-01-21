(ns lob.amqp0.langohr
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [lob.link :as link]
            [lob.message :as msg]))

(defn connect [config conn]
  (if conn
    conn
    (rmq/connect config)))

(defn open-chan [chan conn]
  (if chan
    chan
    (lch/open conn)))

(defn open! [{:keys [conn chan] :as state} config]
  (let [conn (connect config conn)
        chan (open-chan chan conn)]
    (-> state
        (assoc :conn conn)
        (assoc :chan chan))))

(defn close! [{:keys [conn chan] :as state}]
  (when chan
    (rmq/close chan))
  (when conn
    (rmq/close conn))
  (-> state
      (assoc :conn nil)
      (assoc :chan nil)))

(defrecord Link [config state]
  link/Link
  (-open! [_]
    (swap! state open! config)
    true)
  (-close! [_]
    (swap! state close!)
    true)
  (-closed? [_]
    (nil? (:chan @state)))
  (-publication [_ id]))

(defn create-link [config]
  (->Link config (atom {})))
