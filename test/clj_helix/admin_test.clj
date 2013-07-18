(ns clj-helix.admin-test
  (:require [clojure.test :refer :all]
            [clj-helix.admin :refer :all]))


(def h (helix-admin "localhost:2181"))

(deftest basic-test
  (drop-cluster h :helix-test)
  (add-cluster h :helix-test)
  (add-instance h :helix-test {:host "localhost"
                               :port 7000})
  (add-state-model-def h :helix-test :my-state-model
                       {:master {:priority 1
                                 :transitions :slave
                                 :upper-bound 1}
                        :slave  {:priority 2
                                 :transitions [:master :offline]
                                 :upper-bound :R}
                        :offline {:transitions :slave
                                  :initial? true}})
  (add-resource h :helix-test {:resource "a thing"
                               :partitions 6
                               :state-model :my-state-model}))
