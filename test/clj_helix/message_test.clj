(ns clj-helix.message-test
  (:require [clojure.test :refer :all]
            [clj-helix.logging :refer [mute]]
            [clj-helix.fsm :refer [fsm]]
            [clj-helix.admin-test :refer [fsm-def]]
            [clj-helix.manager :refer :all]
            [clj-helix.message :refer :all]))

(use-fixtures :once #(mute (%)))

(defn instance [context]
  (.. context getManager getInstanceName))

(defn make-fsm [state]
  (fsm fsm-def
       (:offline   :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave    :master [p m c] (swap! state assoc [(instance c) p] :master))
       (:master    :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave   :offline [p m c] (swap! state dissoc [(instance c) p]))))

; not sure how this is supposed to work
;(deftest message-test
;  (let [state (atom {})
;        _ (add-watch state :debug (fn [_ _ x y] (prn x y)))
;        instance {:zookeeper "localhost:2181"
;                  :cluster   :helix-test
;                  :instance  {:host "localhost"}}
;        c (controller (assoc-in instance [:instance :port] 7000))
;
;        instance (assoc instance :fsm (make-fsm state))
;        p1 (participant (assoc-in instance [:instance :port] 7001))
;        instance (assoc instance :fsm (make-fsm state))
;        p2 (participant (assoc-in instance [:instance :port] 7002))]
;    (try
;      (Thread/sleep 3000)
;      (prn (matching c (criteria {})))
;
;       (finally
;         (dorun (map disconnect! [p1 p2 c]))))))
