(ns clj-helix.manager
  (:import (org.apache.helix HelixManagerFactory
                             HelixManager
                             InstanceType)
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
  "Creates a new Helix manager for a given cluster. Takes an instance name
  (typically host:port), an instance type (:controller, :participant,
  :spectator, or :admin), and a ZK connection string."
  [cluster-name instance-name instance-type- zk-connect-string]
  (HelixManagerFactory/getZKHelixManager (name cluster-name)
                                         (name instance-name)
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

(defn connect!
  "Connects a HelixManager."
  [^HelixManager manager]
  (.connect manager))
