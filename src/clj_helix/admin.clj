(ns clj-helix.admin
  (:import (org.apache.helix.manager.zk ZKHelixAdmin)
           (org.apache.helix.model InstanceConfig
                                   StateModelDefinition
                                   StateModelDefinition$Builder)))

(defn helix-admin
  "Creates a new helix admin, given a zookeeper address."
  [zookeeper-address]
  (ZKHelixAdmin. zookeeper-address))

(defn instance-config
  "Constructs an InstanceConfig from a map like

  {:name \"localhost_1234\"
   :host \"localhost\"
   :port 1234
   :enabled? true}

  Any other k/v pairs will be added to the config as SimpleFields."
  [m]
  (assert (:host m))
  (assert (:port m))
  (let [config (doto (-> m
                         (get :name (str (:host m) "_" (:port m)))
                         name
                         InstanceConfig.)
                 (.setHostName (:host m))
                 (.setPort (str (:port m)))
                 (.setInstanceEnabled (get m :enabled? true)))
        record (.getRecord config)]
    (doseq [[k v] m]
      (when-not (#{:name :host :port :enabled?} k)
        (.setSimpleField record k v)))
    config))

(defn state-model-definition
  "Creates a new StateModelDefinition. For example:

  (state-model-definition :my-state-model
    {:master {:priority 1
              :transitions :slave
              :upper-bound 1}
     :slave  {:priority 2
              :transitions [:master :offline]
              :upper-bound :R}
     :offline {:transitions :slave
               :initial? true}})"
  [model-name model]
  (assert (= 1 (count (filter :initial? (vals model)))))
  (let [b (StateModelDefinition$Builder. (name model-name))]
    (doseq [[state d] model]
      (let [state (name state)]
        ; Priority
        (if (:priority d)
          (.addState b state (:priority d))
          (.addState b state))
        
        ; Initial state
        (when (:initial? d)
          (.initialState b state))

        ; Transitions
        (let [ts (:transitions d)]
          (doseq [to-state (if (sequential? ts) ts [ts])]
            (.addTransition b state (name to-state))))

       ; Constraints
       (let [bound (:upper-bound d)]
         (cond (nil? bound)    nil
               (number? bound) (.upperBound b state bound)
               :else           (.dynamicUpperBound b state (name bound))))))
   (.build b)))

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
  (.addInstance helix (name cluster-name) (instance-config instance-map))
  helix)

(defn add-state-model-def
  "Adds a state model (as accepted by state-model-definition) to the given
  Helix admin."
  [^ZKHelixAdmin helix cluster-name model-name state-model]
  (.addStateModelDef helix
                     (name cluster-name)
                     (name model-name)
                     (state-model-definition model-name state-model))
  helix)

(defn add-resource
  "Adds a resource to the given helix admin. Helix is a ZKHelixAdmin. Example:
  
  (add-resource helix :my-cluster
                      {:resource :my-db
                       :partitions 6
                       :state-model :MasterSlave
                       :mode :AUTO_REBALANCE})
  
  The default mode is AUTO_REBALANCE."
  [^ZKHelixAdmin helix cluster opts]
  (assert (:resource opts))
  (assert (:partitions opts))
  (assert (:state-model opts))
  (.addResource helix
                (name cluster)
                (name (:resource opts))
                (:partitions opts)
                (name (:state-model opts))
                (name (get opts :mode "AUTO_REBALANCE"))))
