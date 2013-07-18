(ns clj-helix.manager-test
  (:require [clojure.test :refer :all]
            [clj-helix.manager :refer :all]))

(deftest manager-test
  (let [c (helix-manager :my-cluster
                         "localhost:7000"
                         :controller
                         "localhost:2181")
        p (helix-manager :my-cluster
                         "localhost:7001"
                         :participant
                         "localhost:2181")]

    ))
