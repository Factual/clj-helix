(ns clj-helix.manager
  (:use [clj-helix.fsm :only [fsm]]
        [clj-helix.admin :only [instance-name]])
  (:import (org.apache.helix HelixManagerFactory
                             HelixManager
                             InstanceType)
           (org.apache.helix.controller GenericHelixController)
           (org.apache.helix.participant StateMachineEngine)))

(defn instance-type
  "Turns a keyword into an INSTANCE_TYPE enum."
  [t]
  (case t
    :administrator          InstanceType/ADMINISTRATOR
    :controller             InstanceType/CONTROLLER
    :controller-participant InstanceType/CONTROLLER_PARTICIPANT
    :participant            InstanceType/PARTICIPANT
    :spectator              InstanceType/SPECTATOR))

(defn ^HelixManager helix-manager
  "Creates a new Helix manager for a given cluster. Takes a zookeeper connection string, the cluster name, the kind of instance to become (:controller, :participant, :spectator, etc), and an instance map like {:host \"foo\" :port 7000}."
  [zk-connect-string cluster-name instance-type- instance]
  (HelixManagerFactory/getZKHelixManager (name cluster-name)
                                         (instance-name instance)
                                         (instance-type instance-type-)
                                         zk-connect-string))

(defn ^StateMachineEngine state-machine-engine
  "Returns a StateMachineEngine from a helix manager."
  [^HelixManager manager]
  (.getStateMachineEngine manager))

(defn register-state-model-factory!
  "Registers a StateModelFactory, with a given keyword or string name, with
  the given manager's state machine engine."
  [^HelixManager manager model-name state-model-factory]
  (.registerStateModelFactory (state-machine-engine manager)
                              (name model-name)
                              state-model-factory)
  manager)

(defn register-fsm!
  "Registers an FSM with the given manager's state machine engine. clj-helix
  FSMs encode their state model name, so it can be omitted here."
  [^HelixManager manager fsm]
  (register-state-model-factory! manager
                                 (:name (.definition fsm))
                                 fsm))

(defn connect!
  "Connects a HelixManager."
  [^HelixManager manager]
  (.connect manager))

(defn disconnect!
  "Disconnects a HelixManager. Cannot be reused."
  [^HelixManager manager]
  (.disconnect manager))

(defn generic-controller!
  "Given a manager, hooks up a GenericHelixController to it."
  [^HelixManager manager]
  (let [c (GenericHelixController.)]
    (doto manager
      (.addConfigChangeListener c)
      (.addLiveInstanceChangeListener c)
      (.addIdealStateChangeListener c)
      (.addExternalViewChangeListener c)
      (.addControllerListener c))))

(defmacro participant
  "Defines a participant in a distributed system. Takes a zookeeper address,
  the cluster name, and node name. This is followed by a series of state
  machine models which will be registered with the participant. Returns
  a HelixManager. Example:
  
  (participant \"localhost:2181\" :my-cluster \"localhost_7000\")"
  [])
