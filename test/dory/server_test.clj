(ns dory.server-test
  (:require [clojure.test :refer :all]
            [dory.server :refer :all]))

(def a-follower (make-server "test-server" :dory.server/role :follower))

(deftest timeouts-should-be-handled
  (testing "follower becomes a candiate"
    (is (= :candidate (:dory.server/role (handle-timeout a-follower))))))

;; mock handler for logs that accepts two entries and fails on the 3rd
(defn test-log-handler [i]
  ([true true false] i))

(deftest applying-log-entries
  (testing "applying with a vector of entries"
    (is (= 0 (apply-log-entries* [] test-log-handler)))
    (is (= 2 (apply-log-entries* [0 1 2] test-log-handler))))
  (testing "applying with missing arguments"
    (is (= 0 (apply-log-entries* [1 2 3] nil)))
    (is (= 0 (apply-log-entries* nil test-log-handler)))))
