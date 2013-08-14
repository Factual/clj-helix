(ns clj-helix.property-store-test
  (:require [clojure.test :refer :all]
            [clj-helix.logging :refer [mute]]
            [clj-helix.admin-test :refer [controller+ participant+ make-fsm]]
            [clj-helix.manager :as manager]
            [clj-helix.property-store :as prop]))

(use-fixtures :once #(mute (%)))

(deftest store-test
  (let [n1 (participant+ 7001 {})
        n2 (participant+ 7002 {})
        [p1 p2] (map manager/property-store [n1 n2])]
    (try
      (testing "get missing"
        (is (= [nil (prop/stat)] (prop/get* p1 "/hi" :persistent)))
        (is (= nil (prop/get p1 "/hi" :persistent))))

      (testing "cas"
        (is (= 2 (prop/cas! p1 "/hi" :persistent nil 2)))
        (is (= 2 (prop/get p1 "/hi" :persistent))))
      
      (finally
        (dorun (map manager/disconnect! [n1 n2]))))))
