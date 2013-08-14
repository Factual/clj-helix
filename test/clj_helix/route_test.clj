(ns clj-helix.route-test
  (:require [clojure.test :refer :all]
            [clj-helix.logging :refer [mute]]
            [clj-helix.admin-test :refer [controller+ participant+ make-fsm]]
            [clj-helix.admin :refer [instance-name]]
            [clj-helix.manager :refer :all]
            [clj-helix.route :refer :all]))

(use-fixtures :once #(mute (%)))

(deftest route-test
  (let [state (atom {})
        _ (add-watch state :debug (fn [_ _ x y] (prn "-> " y)))
        c (controller+ 7000)
        
        p1 (participant+ 7001 {:fsm (make-fsm state)})
        r1 (router! p1)

        p2 (participant+ 7002 {:fsm (make-fsm state)})
        r2 (router! p2)]
    (try
      (Thread/sleep 3000)
      (prn @state)
      (let [masters (instances r1 "a-thing" :master)]
        (is (= 1 (count masters)))
        (is (= (instance-name (first masters))
               (some
                 (fn [[[instance part] state]]
                   (when (= :master state)
                     instance))
                 @state))))

       (finally
         (dorun (map disconnect! [p1 p2 c]))))))
