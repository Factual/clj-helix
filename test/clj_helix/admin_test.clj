(ns clj-helix.admin-test
  (:require [clojure.test :refer :all]
            [clj-helix.fsm :refer [fsm-definition fsm]]
            [clj-helix.manager :refer [controller participant]]
            [clj-helix.admin :refer :all]))

(def h (helix-admin "localhost:2181"))

(def fsm-def (fsm-definition {:name   :my-state-model
                              :states {:master {:priority 1
                                                :transitions :slave
                                                :upper-bound 1}
                                       :slave  {:priority 2
                                                :transitions [:master :offline]
                                                :upper-bound :R}
                                       :offline {:transitions [:slave :DROPPED]
                                                 :initial? true}
                                       :DROPPED {}}}))

(defn instance [context]
  (.. context getManager getInstanceName))

(defn make-fsm [state]
  (fsm fsm-def
       (:offline   :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave    :master [p m c] (swap! state assoc [(instance c) p] :master))
       (:master    :slave [p m c] (swap! state assoc [(instance c) p] :slave))
       (:slave   :offline [p m c] (swap! state dissoc [(instance c) p]))))

(defn controller+
  "Constructs a new controller."
  [port]
  (controller {:zookeeper "localhost:2181"
               :cluster   :helix-test
               :instance  {:host "localhost"
                           :port port}}))

(defn participant+
  "Constructs a new participant."
  [port opts]
  (participant (merge {:zookeeper "localhost:2181"
                       :cluster   :helix-test
                       :instance  {:host "localhost"
                                   :port port}}
                      opts)))

(deftest basic-test
  (drop-cluster h :helix-test)
  (add-cluster h :helix-test)
  (add-fsm-definition h :helix-test fsm-def)
  (add-resource h :helix-test {:resource "a-thing"
                               :partitions 1
                               :replicas 3
                               :state-model :my-state-model})
  (add-instance h :helix-test {:host "localhost"
                               :port 7000})
  (add-instance h :helix-test {:host "localhost"
                               :port 7001})
  (add-instance h :helix-test {:host "localhost"
                               :port 7002}))
