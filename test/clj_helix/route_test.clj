(ns clj-helix.route-test
  (:require [clojure.test :refer :all]
            [clj-helix.logging :refer [mute]]
            [clj-helix.fsm :refer [fsm]]
            [clj-helix.admin-test :refer [fsm-def]]
            [clj-helix.admin :refer [instance-name]]
            [clj-helix.manager :refer :all]
            [clj-helix.route :refer :all]))

(use-fixtures :once #(mute (%)))

(defn instance [context]
  (.. context getManager getInstanceName))

(defn make-fsm [state]
  (fsm fsm-def
       (:offline   :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave    :master [p m c] (swap! state assoc [(instance c) p] :master))
       (:master    :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave   :offline [p m c] (swap! state dissoc [(instance c) p]))))

(deftest route-test
  (let [state (atom {})
        _ (add-watch state :debug (fn [_ _ x y] (prn "-> " y)))
        instance {:zookeeper "localhost:2181"
                  :cluster   :helix-test
                  :instance  {:host "localhost"}}
        c (controller (assoc-in instance [:instance :port] 7000))

        instance (assoc instance :fsm (make-fsm state))
        p1 (participant (assoc-in instance [:instance :port] 7001))
        r1 (router! p1)

        instance (assoc instance :fsm (make-fsm state))
        p2 (participant (assoc-in instance [:instance :port] 7002))
        r2 (router! p2)]
    (try
      (Thread/sleep 3000)
      (let [masters (instances r1 "a-thing" :master)]
        (is (= 1 (count masters)))
        (is (= (instance-name (first masters))
               (some
                 (fn [[[instance part] state]]
                   (when (= :master state)
                     instance))
                 @state))))

;      (let [peers (instances r1 "a-thing" :*)]
;        (is (= 2 (count peers)))
;        (is (= (set (map instance-name peers))
;               (set (map (comp first key) @state)))))

       (finally
         (dorun (map disconnect! [p1 p2 c]))))))
