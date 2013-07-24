(ns clj-helix.fsm-test
  (:require [clojure.test :refer :all]
            [clj-helix.fsm :refer :all]))

(deftest fsm-test
  (let [x 2
        d (fsm-definition {:name   :lightswitch
                           :states {:on  {}
                                    :off {:initial? true}}})
        f (fsm d
            (:on  :off [part msg ctx] [x :off part])
            (:off :on  [part msg ctx] [x :on part]))
        m (.createNewStateModel f "p1")]

    (testing "initial state"
      (is (= :off (initial-state d))))

    (testing "state model"
      (is (= [2 :on "p1"] (.onBecomeonFromoff m nil nil))))))
