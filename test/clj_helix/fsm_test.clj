(ns clj-helix.fsm-test
  (:require [clojure.test :refer :all]
            [clj-helix.fsm :refer :all]))

(deftest reachable-states-test
  (let [graph {:states {:a {:transitions [:a]}
                        :b {:transitions [:a]}
                        :c {:transitions [:b]}
                        :d {:transitions [:b :e]}
                        :e {:transitions [:c]}}}]
    (is (= (reachable-states graph :a) #{:a}))
    (is (= (reachable-states graph :b) #{:a :b}))
    (is (= (reachable-states graph :c) #{:a :b :c}))
    (is (= (reachable-states graph :e) #{:e :c :b :a}))
    (is (= (reachable-states graph :d) #{:a :b :c :d :e}))))

(deftest fsm-test
  (let [x 2
        d (fsm-definition {:name   :lightswitch
                           :states {:on  {:transitions :off}
                                    :off {:initial? true
                                          :transitions [:on :DROPPED]}
                                    :DROPPED {}}})
        f (fsm d
            (:on  :off [part msg ctx] [x :off part])
            (:off :on  [part msg ctx] [x :on part]))
        m (.createNewStateModel f "p1")]

    (testing "adjacency-map"
      (is (= {:on [:off]
              :off [:on :DROPPED]
              :DROPPED []}
             (adjacency-map d))))

    (testing "initial state"
      (is (= :off (initial-state d))))

    (testing "state model"
      (is (= [2 :on "p1"] (.onBecomeonFromoff m nil nil))))))
