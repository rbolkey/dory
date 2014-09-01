(ns dory.server
  (:require [clojure.core.async :as async :refer [<! >! timeout chan alt! alts! go go-loop]]))

;;;;;;;;;;;;;;;;;;
;; DATA STRUCTURES

(defn log-entry [index term]
  (hash-map :index index
            :term term
            :command nil))

(defn make-append-entries-request [svr & r]
  (merge {::message-name       :append-entries-request
          ;; leader's term
          :term                (:current-term svr)
          ;; so the follower can redirect clients
          :leader              (::name svr)
          ;; index of log entry immediately preceding new ones
          :prev-log-index      nil
          ;; term of :prev-log-index
          :prev-log-term       nil
          ;; log entries to store (empty for heartbeat); may send more than one
          ;; for efficiency
          :entries             []
          ;; leader's commit-index
          :leader-commit-index (get-in svr [:log :commit-index])}
         (apply hash-map r)))

(defn make-append-entries-response [svr & r]
  (merge {::message-name :append-entries-response
          ;; current term, for the leader to update itself
          :term          (:current-term svr)
          ;; true if follower contained entry matching prev-log-index and prev-log-term
          :success       false}
         (apply hash-map r))
  )

;; Define the map that represents the server object
(defn make-server [name & r]
  (let [svr (merge {::name              name
                    ;; the current role of this server
                    ::role              :stopped
                    ;; latest term server has seen
                    :current-term       1
                    ;; leader for the current term
                    :current-leader     nil
                    ;; candidateId that received vote in current term
                    :voted-for          nil
                    ;; index of next log entry to send to that server (leader role)
                    :next-index         {}
                    ;; index of highest log entry known to be replicated on server (leader role)
                    :match-index        {}
                    ;; period of time in which a follower must receive communication from the leader
                    :election-timeout   150000
                    ;; time between heartbeats sent by the leader for it to remain authoritive
                    :heartbeat-interval 50000
                    ;; channel for receiving commands
                    :command-ch         (chan 256)
                    ;; channel for stopping the server
                    :stopping-ch        (chan)
                    ;; channel for responding events from state changes
                    :event-ch           (chan 256)}
                   ;; submap that represents the state of the log
                   {:log {
                           ;; log entries; applied to state machine. This will likely need to be pluggable in the future
                           :entries      []
                           ;; fn called to apply entries to the state machine
                           :apply-fn     nil
                           ;; index of highest log entry known to be committed
                           :commit-index 0
                           ;; index of highest log entry applied to state machine
                           :last-applied 0}}
                   ;; supply additional attributes or overrides
                   (apply hash-map r))]
    svr))

(defn role-transitions []
  "Defines map of server role transition functions. Not sure if useful yet."
  {:stopped   {}
   :follower  {}
   :candidate {}
   :leader    {}})

(defmulti message-timeout "Creates a timeout channel for message handling by role." ::role)
(defmulti handle-timeout "Handles the message handling timeout by server role." ::role)
(defmulti good-message? "Checks that the rpc message is meets preconditions." ::message-name)
(defmulti handle-message "Handles an RPC message given the server's role and message's name."
          (fn [msg svr] (vector (msg ::message-name) (svr ::role))))

(defmethod message-timeout :follower follower-timeout [svr]
                                                      ; TODO parameterize the election timeout value
                                                      (timeout 1000))

(defmethod handle-timeout :follower handle-follower-timeout [svr]
                                                            ; Promote the follower to candidate
                                                            (assoc svr ::role :candidate))

(defn contains-log-entry?
  "Returns true if the server contains a log entry with the given term and index."
  [svr index term]
  (some #(and (== index (:index %)) (== term (:term %)))
        (take-while #(and (<= index (:index %) (<= term (:term %))))
                    (reverse (get-in svr [:log :entries])))))

(defn truncate-log-entries [svr index term]
  svr)

(defn valid-append-entry?
  "Returns true if the entry is consistent with the last entry appended."
  [entry prev-entry]
  (or (not prev-entry) (and (>= (:term entry) (:term prev-entry))
                            (> (:index entry) (:index prev-entry)))))

(defn append-log-entries
  "Appends the new log entries to the server's logs."
  [{{appended-entries :entries} :log :as svr} new-entries]
  (let [entry (first new-entries)
        last-entry (peek appended-entries)]
    (if entry
      (recur (assoc-in svr [:log :entries]
                       (if (valid-append-entry? entry last-entry)
                         (conj appended-entries entry)
                         appended-entries))
             (rest new-entries))
      svr)))

(defn update-commit-index [svr commitIndex]
  (assoc-in svr [:log :commit-index] commitIndex))

(defmethod good-message? :append-entries-request good-append-entries-request? [msg]
                                                                              (and (:term msg) (:leader msg)))

(defmethod handle-message [:append-entries-request :follower] follower-append-entries-request
  [msg svr]
  {:pre [(good-message? msg)]}
  (let [curTerm (:current-term svr)
        reqTerm (:term msg)
        reqLeader (:leader msg)
        prevIndex (:prev-log-index msg)
        prevTerm (:prev-log-term msg)
        reqCommitIndex (:leader-commit-index msg)]
    (if (and (<= curTerm reqTerm)
             (contains-log-entry? svr prevIndex prevTerm))
      ;; TODO reply true message
      (-> svr
          (truncate-log-entries prevIndex prevTerm)
          (append-log-entries (:entries msg))
          (assoc :current-term reqTerm)
          (assoc :current-leader reqLeader)
          (update-commit-index svr reqCommitIndex))
      ;; TODO reply false message
      svr)))

; (defmethod handle-message [:append-entries-request :candidate] [msg svr])
; (defmethod handle-message [:append-entries-request :leader] [msg svr])
; (defmethod handle-message [:append-entries-response :leader] [msg svr])
; (defmethod handle-message [:request-vote-request :follower] [msg svr])
; (defmethod handle-message [:request-vote-request :candidate] [msg svr])
; (defmethod handle-message [:request-vote-request :leader] [msg svr])
; (defmethod handle-message [:request-vote-response :candidate] [msg svr])
; (defmethod handle-message [:snapshot-request :follower] [msg svr])
; (defmethod handle-message [:join-request :follower] [msg svr])
; (defmethod handle-message [:stop-request :follower] [msg svr])
; (defmethod handle-message [:stop-request :candidate] [msg svr])
; (defmethod handle-message [:stop-request :leader] [msg svr])

(defn make-response [msg svr])

(defn deep-merge
  "Recursively merges maps. If vals are not maps, the last value wins."
  [& vals]
  (if (every? map? vals)
    (apply merge-with deep-merge vals)
    (last vals)))

(defn update-server!
  "Updates the server state atom by merging in the server state in new-val."
  [svr-atom new-val]
  (swap! svr-atom deep-merge new-val))

(defn throw-err [e]
  (when (instance? Throwable e) (throw e))
  e)

(defmacro <? [ch]
  `(throw-err (/ <! ~ch)))

;; loops through rpc messages sent to the server while updating the server's state
(defn server-loop! [svr]
  (go-loop []
           (if (= :stopped (::role svr))
             svr
             ;; TODO handle exceptions lost in go block
             (let [t (message-timeout svr)]
               (alt!
                 ;; handle stop
                 [(:stopping-ch svr)]
                 ([v]
                  (update-server! (:state svr)
                                  (fn [s]
                                    (let [handled (handle-message s v)]
                                      (deep-merge s handled)))))
                 ;; handle timeout
                 [t]
                 ([v] (handle-timeout svr))
                 ;; handle message
                 [(:command-ch svr)
                  (:stopping-ch svr)]
                 ([v] (let [new-state
                            (update-server! (:state svr)
                                            (fn [s]
                                              (deep-merge s (handle-message s v))))]
                        (>! (:event-ch svr) (make-response new-state v)))))
               (recur)))))

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

