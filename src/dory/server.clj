(ns dory.server
  (:require [clojure.core.async :as async :refer [<! >! timeout chan alt! alts! go go-loop]]))

;;;;;;;;;;;;;;;;;;
;; DATA STRUCTURES

(defn log-entry [index term]
  (hash-map :index index
            :term term
            :command nil))

(defn make-append-entries-request [svr & r]
  (let [svr @svr
        cmd (merge {::command-name :append-entries
                    ;; leader's term
                    :term (:current-term svr)
                    ;; so the follower can redirect clients
                    :leader-id (::name svr)
                    ;; index of log entry immediately preceding new ones
                    :prev-log-index nil
                    ;; term of :prev-log-index
                    :prev-log-term nil
                    ;; log entries to store (empty for heartbeat); may send more than one
                    ;; for efficiency
                    :entries []
                    ;; leader's commit-index
                    :leader-commit (get-in svr [:log :commit-index])}
                   (apply hash-map r))]
    cmd))

;; Define the map that represents the server object
(defn make-server [name & r]
  (let [svr (merge {::name name
                    ;; the current role of this server
                    ::role :stopped
                    ;; latest term server has seen
                    :current-term 1
                    ;; candidateId that received vote in current term
                    :voted-for nil
                    ;; index of next log entry to send to that server (leader role)
                    :next-index {}
                    ;; index of highest log entry known to be replicated on server (leader role)
                    :match-index {}
                    ;; period of time in which a follower must receive communication from the leader
                    :election-timeout 150000
                    ;; time between heartbeats sent by the leader for it to remain authoritive
                    :heartbeat-interval 50000
                    ;; channel for sending/receiving commands
                    :ch (chan 256)
                    ;; channel for stopping the server
                    :stopped (chan) }
                   ;; submap that represents the state of the log
                   {:log {
                          ;; log entries; applied to state machine. This will likely need to be pluggable in the future
                          :entries []
                          ;; fn called to apply entries to the state machine
                          :apply-fn nil
                          ;; index of highest log entry known to be committed
                          :commit-index 0
                          ;; index of highest log entry applied to state machine
                          :last-applied 0}}
                   ;; supply additional attributes or overrides
                   (apply hash-map r) )]
    svr))

;; returns a timeout channel based on the follower's election timeout
(defn election-timeout [svr])

(defn follower-handle-timeout [svr]
  (assoc svr :state :candidate))

(defn follower-handle-stop [svr])

(defn follower-handle-request [svr cmd])

(defn follower-loop! [svr]
  (let [ch (:ch @svr)
        st (:stopped @svr)]
    (go-loop [t (election-timeout svr)
              work (= (:state @svr) :follower)]
             (when work
               (recur (election-timeout svr)
                      (and (alt! 
                             ;; handle stop
                             [st] ([v] (follower-handle-stop svr))
                             ;; handle a timeout
                             [t] ([v] (follower-handle-timeout svr))
                             ;; handle a command
                             [ch] ([v] (follower-handle-request svr v)))
                           (= (:state @svr) :follower)))))  ))

;; loops through the entries, calling the given function for each until the function fails, returns the number
;; of successful calls to f
(defn apply-log-entries* [entries f]
  (letfn [(apply-log-entry [entries applied]
            (let [entry (first entries)]
              (if (and f entry (f entry))
                (recur (rest entries) (inc applied))
                applied)))]
    (apply-log-entry entries 0)))

;; destructures the svr in order to apply log entries to the state machine, and updates the last-applied value
(defn apply-log-entries
  [svr]
  (let [log (:log @svr)
        last-applied (:last-applied log)
        unapplied-committed-entries (drop last-applied (take (:commit-index log) (:entries log)))
        apply-fn (:apply-fn log)
        applied (apply-log-entries unapplied-committed-entries apply-fn)]
    (swap! svr update-in [:log :last-applied] (fn [oldval] (+ last-applied applied)))))

