(ns lob.amqp0.langohr
  (:require [langohr.core :as rmq]
            [langohr.channel :as lch]
            [langohr.queue :as lq]
            [langohr.consumers :as lc]
            [langohr.basic :as lb]
            [langohr.exchange  :as le]
            [lob.link :as link]
            [lob.message :as msg]
            [com.stuartsierra.component :as component]))

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

(defrecord MessageIn [chan src dst meta payload]
  msg/Message
  (-id [_]
    (:message-id meta))
  (-source [_]
    src)
  (-destination [_]
    dst)
  (-content-type [_]
    (:content-type meta))
  (-headers [_]
    (:headers meta))
  (-content [_]
    payload)
  msg/Ackable
  (-ack! [_]
    (lb/ack chan (:delivery-tag meta)))
  msg/Nackable
  (-nack! [_]
    (lb/nack chan (:delivery-tag meta) false true)))

(defrecord PushSubscription [chan tag]
  link/Subscription
  (-unsubscribe! [_]
    (try
      (lb/cancel chan tag)
      (catch Exception e
        nil))))

(defrecord PullSubscription [chan src sub-id]
  link/Subscription
  (-unsubscribe! [_])
  link/Receiver
  (-receive! [_]
    (let [[metadata ^bytes payload] (lb/get chan sub-id false)]
      (when payload
        (->MessageIn chan src sub-id metadata (String. payload "UTF-8"))))))

(defrecord Publication [link chan key sub-id pub-id]
  link/Publication
  (-subscribe! [_ buffer-size callback]
    (let [opts {:auto-ack false}]
      (if (fn? callback)
        (do
          (lb/qos chan buffer-size)
          (let [tag (lc/subscribe chan
                                  sub-id
                                  (fn [ch meta ^bytes payload]
                                    (callback (->MessageIn chan
                                                           (link/id link)
                                                           sub-id
                                                           meta
                                                           (String. payload "UTF-8"))))
                                  opts)]
            (->PushSubscription chan tag)))
        (->PullSubscription chan (link/id link) sub-id))))
  link/Sender
  (-send! [_ msg]
    (if (link/closed? link)
      false
      (let [id (msg/id msg)
            content (str (msg/content msg)) ;; FIXME: Needs encoding
            opts {:content-type (msg/content-type msg)
                  :message-id id}]
        (lb/publish chan "" pub-id content opts))))
  component/Lifecycle
  (start [this]
    (if chan
      this
      (if-let [pub (link/publication link pub-id)]
        pub
        (throw (ex-info "Could not create publication"
                        {:link link
                         :link-closed? (link/closed? link)
                         :pub-id pub-id})))))
  (stop [this]
    (map->Publication {:link link
                       :pub-id pub-id})))

(defn publication [id]
  (map->Publication {:pub-id id}))

(defrecord Link [config state]
  link/Link
  (-id [_]
    (:host config))
  (-open! [_]
    (swap! state open! config)
    true)
  (-close! [_]
    (swap! state close!)
    true)
  (-closed? [_]
    (nil? (:chan @state)))
  (-publication [this id opts]
    (when-not (link/closed? this)
      (let [{:keys [multicast persistent durable key]} opts
            {:keys [chan]} @state
            config {:durable durable
                    :exclusive false
                    :auto-delete (not persistent)}
            ex-type (if (true? multicast) "fanout" multicast)]
        (if multicast
          (do
            (le/declare chan id ex-type config)
            (let [q-config {:exclusive false
                            :auto-delete true}
                  queue (lq/declare-server-named chan q-config)]
              (lq/bind chan queue id)
              (->Publication this chan key queue id)))
          (do
            (lq/declare chan id config)
            (->Publication this chan key id id))))))
  component/Lifecycle
  (start [this]
    (when (link/closed? this)
      (link/open! this))
    this)
  (stop [this]
    (when-not (link/closed? this)
      (link/close! this))
    this))

(defn create-link [config]
  (->Link config (atom {})))
