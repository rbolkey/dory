(ns dory.server-test
  (:require [clojure.test :refer :all]
            [dory.server :refer :all]))

(deftest server-definition
  (testing "Defining a new server."
    (is (= (server* :my-server) 1))))


