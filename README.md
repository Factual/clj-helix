# clj-helix

A Clojure wrapper around [Apache Helix](http://helix.incubator.apache.org/), a
library for managing cluster state in distributed systems. API is incomplete
and subject to change, but the basics are in place and work correctly.

## Installation

Via [clojars](https://clojars.org/factual/clj-helix).

## Usage

``` clj
(use 'clj-helix.admin)
; Create an admin object around Zookeeper
(def admin (helix-admin "localhost:2181"))

; Create a cluster
(add-cluster admin :my-app)

; Define a finite state machine
(def fsm-def
 (clj-helix.fsm/fsm-definition
  {:name :my-app-fsm
   :states {:DROPPED {:transitions :offline}
   :offline {:initial? true
   :transitions [:peer :DROPPED]}
   :peer {:priority 1
   :upper-bound :R
   :transitions :offline}}}))

; Add the FSM definition to the cluster
(add-fsm-definition admin :my-app fsm-def)

; Add a resource
(add-resource admin :my-app {:resource   :some-resouce
                             :partitions 128
                             :replicas   5
                             :state-model (:name fsm-def))

; Add a few nodes
(add-instance admin :my-app {:host "10.0.0.1"
                             :port 1234})
(add-instance admin :my-app {:host "10.0.0.2"
                             :port 1234})

; Now you can start controllers and/or participants for those nodes:
(use 'clj-helix.manager)

; Create a controller, which will coordinate state transitions. You typically
; want a few of these per cluster for redundancy.
(def c (controller {:zookeeper "localhost:2181"
                    :cluster   :my-app
                    :host      "10.0.0.1"
                    :port      1234})) 

; Create a participant, which owns resources and handles state transitions.
; First, we need an FSM to handle those state transitions we defined earlier.
; (fsm) compiles a new class with the appropriate Helix annotations. Handler
; bodies are lexical closures.
(def f (clj-helix.fsm/fsm fsm-def
         (:offline :peer [part message context]
           (prn part :coming-online))

         (:offline :DROPPED [part m c]
           (prn part "dropped!"))

         (:peer :offline [part m c]
           (prn part "Going offline."))))


; Create participant
(def p (participant {:zookeeper "localhost:2181"
                     :cluster   :my-app
                     :host      "10.0.0.1"
                     :port      1234
                     :fsm       fsm}))

; The participant will start reacting to state changes coordinated by the
; controller.

 When you're ready to quit, shut down the participant or
; controller:
(shutdown! p)
(shutdown! c)

; The router can help figure out where to send messages:
(use 'clj-helix.router)
(def r (router! p))

; Find all peers
(instances r :my-app :some-resouce)

; Find peers for a specific partition
(instances r :my-app :some-resouce "some-resource_2")

## License

Copyright © 2013 Factual, Inc

Distributed under the Eclipse Public License, the same as Clojure.
