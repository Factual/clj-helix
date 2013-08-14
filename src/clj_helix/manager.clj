(ns clj-helix.manager
  (:use [clj-helix.fsm :only [fsm]]
        [clj-helix.admin :only [instance-name]])
  (:import (clj_helix.fsm FSM)
           (org.apache.helix HelixManagerFactory
                             HelixManager
                             HelixAdmin
                             InstanceType)
           (org.apache.helix.store.zk ZkHelixPropertyStore)
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
  [^HelixManager manager ^FSM fsm]
  (register-state-model-factory! manager
                                 (:name (.definition fsm))
                                 fsm))

(defn connect!
  "Connects a HelixManager."
  [^HelixManager manager]
  (.connect manager)
  manager)

(defn disconnect!
  "Disconnects a HelixManager. Cannot be reused."
  [^HelixManager manager]
  (.disconnect manager)
  manager)

(defn ^HelixManager helix-manager
  "Creates a new Helix manager for a given cluster. Options:
  
  :zookeeper      a zookeeper connection string
  :cluster        the cluster name
  :type           the kind of instance to become (:controller, etc)
  :instance       this instance's identity; e.g. {:host \"foo\" :port 7000}.
  :fsms (or :fsm) a list of FSMs to register."
  [opts]
  (let [m (HelixManagerFactory/getZKHelixManager (name (:cluster opts))
                                                 (instance-name (:instance opts))
                                                 (instance-type (:type opts))
                                                 (:zookeeper opts))]
    (when (:fsm opts) (register-fsm! m (:fsm opts)))
    (dorun (map (partial register-fsm! m) (:fsms opts)))
    m))

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

(defn controller
  "Sets up a controller manager with the default GenericController; args as for
  (helix-manager). Returns the manager, already connected."
  [opts]
  (-> (assoc opts :type :controller)
      helix-manager
      connect!
      generic-controller!))

(defn participant
  "Defines a participant in a distributed system; args as for (helix-manager).
  Returns a manager, already connected."
  [opts]
  (-> (assoc opts :type :participant)
      helix-manager
      connect!))

(defn ^HelixAdmin admin
  "Returns a HelixAdmin associated with a manager."
  [^HelixManager manager]
  (.getClusterManagmentTool manager))

(defn cluster-name
  "Gets the name of the cluster this manager is associated with."
  [^HelixManager manager]
  (.getClusterName manager))

(defn ^ZkHelixPropertyStore property-store
  "Returns the PropertyStore for this manager."
  [^HelixManager manager]
  (.getHelixPropertyStore manager))
