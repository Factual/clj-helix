(ns clj-helix.admin-test
  (:require [clojure.test :refer :all]
            [clj-helix.fsm :refer [fsm-definition]]
            [clj-helix.admin :refer :all]))

(def h (helix-admin "localhost:2181"))

(def fsm-def (fsm-definition {:name   :my-state-model
                              :states {:master {:priority 1
                                                :transitions :slave
                                                :upper-bound 1}
                                       :slave  {:priority 2
                                                :transitions [:master :offline]
                                                :upper-bound :R}
                                       :offline {:transitions :slave
                                                 :initial? true}}}))

(deftest basic-test
  (drop-cluster h :helix-test)
  (add-cluster h :helix-test)
  (add-instance h :helix-test {:host "localhost"
                               :port 7000})
  (add-instance h :helix-test {:host "localhost"
                               :port 7001})
  (add-instance h :helix-test {:host "localhost"
                               :port 7002})
  (add-fsm-definition h :helix-test fsm-def)
  (add-resource h :helix-test {:resource "a-thing"
                               :partitions 1
                               :replicas 3
                               :state-model :my-state-model}))
