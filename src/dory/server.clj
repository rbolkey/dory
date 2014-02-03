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

