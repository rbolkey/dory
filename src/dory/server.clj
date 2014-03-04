(ns dory.server
  (:require [clojure.core.async :as async :refer [<! >! timeout chan alt! alts! go go-loop]]))

;;;;;;;;;;;;;;;;;;
;; DATA STRUCTURES

(defn log-entry [index term]
  (hash-map :index index
            :term term
            :command nil))

(defn make-append-entries-request [svr & r]
  (merge {::message-name :append-entries-request
                    ;; leader's term
                    :term (:current-term svr)
                    ;; so the follower can redirect clients
                    :leader (::name svr)
                    ;; index of log entry immediately preceding new ones
                    :prev-log-index nil
                    ;; term of :prev-log-index
                    :prev-log-term nil
                    ;; log entries to store (empty for heartbeat); may send more than one
                    ;; for efficiency
                    :entries []
                    ;; leader's commit-index
                    :leader-commit-index (get-in svr [:log :commit-index])}
                   (apply hash-map r)))

;; returns true if the server contains a log entry with given term and index
(defn contains-log-entry? [svr term index]
  ; (some #() (take-while #() (reverse ))) 
  false)

(defn make-append-entries-response [svr & r]
  (merge {::message-name :append-entries-response
          ;; current term, for the leader to update itself
          :term (:current-term svr)
          ;; true if follower contained entry matching prev-log-index and prev-log-term
          :success false} 
         (apply hash-map r))
  )

;; Define the map that represents the server object
(defn make-server [name & r]
  (let [svr (merge {::name name
                    ;; the current role of this server
                    ::role :stopped
                    ;; latest term server has seen
                    :current-term 1
                    ;; leader for the current term
                    :current-leader nil
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

(defn contains-log-entry? [svr index term] 
  false)
(defn truncate-log-entries [svr index term]
  svr)
(defn append-log-entries [svr entries]
  svr)
(defn update-commit-index [svr commitIndex])

(defmulti good-message? ::message-name)
(defmethod good-message? :default [msg]
  false)
(defmethod good-message? :append-entries-request [msg]
  (and (:term msg) (:leader msg)))

;; multimethod to route messages based on server state and message type
(defmulti handle-message (fn [svr msg] (vector (svr ::role) (msg ::message-name))))

(defmethod handle-message :default [svr msg]
  (vector svr nil))

(defmethod handle-message [:follower :append-entries-request] [svr msg]
  {:pre [(good-message? msg)]}
  (let [curTerm (:current-term svr)
        reqTerm (:term msg)
        reqLeader (:leader msg)
        prevIndex (:prev-log-index msg)
        prevTerm (:prev-log-term msg)
        reqCommitIndex (:leader-commit-index msg) ]
    (if (or (< reqTerm curTerm) 
            (contains-log-entry? svr prevIndex prevTerm))
      (vector svr (make-append-entries-response svr))
      (vector (-> svr
                  (truncate-log-entries prevIndex prevTerm)
                  (append-log-entries (:entries msg))
                  (assoc :current-term reqTerm)
                  (assoc :current-leader reqLeader)
                  (update-commit-index svr reqCommitIndex)) 
              make-append-entries-response :success true))))

(defmethod handle-message [:candidate :append-entries-request] [svr msg])

(defmethod handle-message [:leader :append-entries-request] [svr msg])

(defmethod handle-message [:leader :append-entries-response] [svr msg])

; (defmethod handle-message [:follower :request-vote-request] [svr msg])
; (defmethod handle-message [:candidate :request-vote-request] [svr msg])
; (defmethod handle-message [:leader :request-vote-request] [svr msg])
; (defmethod handle-message [:candidate :request-vote-response] [svr msg])
; (defmethod handle-message [:follower :snapshot-request] [svr msg])
; (defmethod handle-message [:follower :join-request] [svr msg])
; (defmethod handle-message [:follower :stop-request] [svr msg])
; (defmethod handle-message [:candidate :stop-request] [svr msg])
; (defmethod handle-message [:leader :stop-request] [svr msg])

(defn deep-merge [& vals]
  (if (every? map? vals)
    (apply merge-with deep-merge vals)
    (last vals)))

(defn update-server! [svr-atom new-val]
  (swap! svr-atom deep-merge new-val))

;; loops through rpc messages sent to the server while updating the server's state
(defn server-loop! [svr]
  (go-loop [svr svr]
           (let [s @svr
                 ch (:ch s)
                 st (:stopped s)
                 t (election-timeout s)]
             (recur (update-server! svr (alt!
                                 ;; handle stop
                                 [st] ([v] (handle-message s v))
                                 ;; handle timeout
                                 ;;[t] ([v] (handle-timeout s))
                                 ;; handle message
                                 [ch] ([v] (handle-message s v))) )))))

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

