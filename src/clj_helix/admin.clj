(ns clj-helix.admin
  "Supports administrative tasks around cluster management."
  (:use [clj-helix.fsm :only [state-model-definition]])
  (:import (org.apache.helix.manager.zk ZKHelixAdmin)
           (org.apache.helix.model InstanceConfig
                                   StateModelDefinition
                                   StateModelDefinition$Builder)))

(defn helix-admin
  "Creates a new helix admin, given a zookeeper address."
  [zookeeper-address]
  (ZKHelixAdmin. zookeeper-address))

(defn instance-name
  "Turns an instance map like {:host \"foo\" :port 7000} into a node identifier
  like \"foo_7000\"."
  [node]
  (assert (:host node))
  (assert (:port node))
  (str (:host node) ":" (:port node)))

(defn instance-config->map
  "Turns an InstanceConfig into a normal Clojure map."
  [^InstanceConfig instance]
  {:host     (.getHostName instance)
   :port     (.getPort instance)
   :enabled? (.getInstanceEnabled instance)})

(defn make-instance-config
  "Constructs an InstanceConfig from a map like

  {:host \"localhost\"
   :port 1234
   :enabled? true}

  Any other k/v pairs will be added to the config as SimpleFields."
  [m]
  (assert (:host m))
  (assert (:port m))
  (let [config (doto (-> m instance-name InstanceConfig.)
                 (.setHostName (:host m))
                 (.setPort (str (:port m)))
                 (.setInstanceEnabled (get m :enabled? true)))
        record (.getRecord config)]
    (doseq [[k v] m]
      (when-not (#{:name :host :port :enabled?} k)
        (.setSimpleField record k v)))
    config))

(defn add-cluster
  "Adds a new cluster to a helix manager. Idempotent."
  [^ZKHelixAdmin helix cluster-name]
  (.addCluster helix (name cluster-name))
  helix)

(defn drop-cluster
  "Removes a cluster from a helix manager."
  [^ZKHelixAdmin helix cluster-name]
  (.dropCluster helix (name cluster-name))
  helix)

(defn add-instance
  "Adds an instance map to a HelixAdmin."
  [^ZKHelixAdmin helix cluster-name instance-map]
  (.addInstance helix (name cluster-name) (make-instance-config instance-map))
  helix)

(defn add-fsm-definition
  "Adds an FSM definitio to the given Helix admin."
  [^ZKHelixAdmin helix cluster-name fsm-def]
  (.addStateModelDef helix
                     (name cluster-name)
                     (name (:name fsm-def))
                     (state-model-definition fsm-def))
  helix)

(defn add-resource
  "Adds a resource to the given helix admin. Helix is a ZKHelixAdmin. Example:
  
  (add-resource helix :my-cluster
                      {:resource :my-db
                       :partitions 6
                       :replicas 5
                       :state-model :MasterSlave
                       :mode :AUTO_REBALANCE})
  
  The default number of partitions is one.
  The default number of replicas is three.
  The default mode is AUTO_REBALANCE."
  [^ZKHelixAdmin helix cluster opts]
  (assert (:resource opts))
  (assert (:state-model opts))
  (.addResource helix
                (name cluster)
                (name (:resource opts))
                (get opts :partitions 1)
                (name (:state-model opts))
                (name (get opts :mode "AUTO_REBALANCE")))
  (.rebalance helix
              (name cluster)
              (name (:resource opts))
              (get opts :replicas 3)))

(defn clusters
  "Returns a list of clusters under /."
  [^ZKHelixAdmin helix]
  (.getClusters helix))

(defn instances
  "A list of the instances in a given cluster."
  [^ZKHelixAdmin helix cluster-name]
  (.getInstancesInCluster helix (name cluster-name)))

(defn instance-config
  "The config for an instance, by name."
  [^ZKHelixAdmin helix cluster-name instance-name]
  (.getInstanceConfig helix (name cluster-name) (name instance-name)))

(defn resources
  "A list of resources."
  [^ZKHelixAdmin helix cluster-name]
  (.getResourcesInCluster helix (name cluster-name)))

(defn resource-ideal-state
  "The ideal state for a resource."
  [^ZKHelixAdmin helix cluster-name resource-name]
  (.getResourceIdealState helix (name cluster-name) (name resource-name)))

(defn resource-external-view
  [^ZKHelixAdmin helix cluster-name resource-name]
  (.getResourceExternalView helix (name cluster-name) (name resource-name)))
