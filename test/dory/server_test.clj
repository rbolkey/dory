(ns dory.server-test
  (:require [clojure.test :refer :all]
            [dory.server :refer :all]))

(defn test-log-handler [i]
  ([true true false] i))

(deftest applying-log-entries
  (testing "applying with a vector of entries"
    (is (= 0 (apply-log-entries* [] test-log-handler)))
    (is (= 2 (apply-log-entries* [0 1 2] test-log-handler))))
  (testing "applying with missing arguments"
    (is (= 0 (apply-log-entries* [1 2 3] nil)))
    (is (= 0 (apply-log-entries* nil test-log-handler))))
  )
