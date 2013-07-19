(ns clj-helix.state-model-test
  (:require [clojure.test :refer :all]
            [clj-helix.state-model :refer :all]))

(deftest state-model-class-test
  (let [x 2
        f (state-model-factory [:off :on]
            (:on  :off [part msg ctx] [x :off part])
            (:off :on  [part msg ctx] [x :on part]))
        m (.createNewStateModel f "p1")]
    (is (= [2 :on "p1"] (.onBecomeonFromoff m nil nil)))))
