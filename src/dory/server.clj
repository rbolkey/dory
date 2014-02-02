(ns dory.server)

;;;;;;;;;;;;;;;;;;
;; DATA STRUCTURES

;; Define the map that represents the server object
(defn make-server [name & r]
  (let [svr (merge {::name name
                    ;; the current role of this server
                    ::role :follower
                    ;; latest term server has seen
                    :current-term 1
                    ;; candidateId that received vote in current term
                    :voted-for nil
                    ;; index of next log entry to send to that server (leader role)
                    :next-index {}
                    ;; index of highest log entry known to be replicated on server (leader role)
                    :match-index {} }
                   ;; submap that represents the state of the log
                   {:log {
                          ;; log entries; applied to state machine
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

;; loops through the unapplied committed entries and calls the log's :apply-fn 
;; returns the index of the last applied log entry
(defn apply-log-entries [svr]
  (let [log (:log @svr)
        last-applied (:last-applied log)
        unapplied-committed-entries (drop last-applied (take (:commit-index log) (:entries log)))
        apply-fn (:apply-fn log)]
    (letfn [(apply-log-entry [entries cur]
              (let [entry (first entries) 
                    nxt (inc cur)]
                (if (and apply-fn entry (apply-fn entry)) 
                  (do 
                    (swap! svr update-in [:log :last-applied] (fn [oldval] nxt))
                    (recur (rest entries nxt)))
                  cur)))]
      (apply-log-entry unapplied-committed-entries last-applied))))
