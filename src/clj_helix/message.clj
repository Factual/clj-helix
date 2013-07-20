(ns clj-helix.message
  (:import (org.apache.helix ClusterMessagingService
                             Criteria
                             HelixManager)
           (org.apache.helix.model Message)
           (org.apache.helix.messaging AsyncCallback
                                       CriteriaEvaluator)))

;; Unfinished

(defmacro callback
  "Helix uses an AsyncCallback class to receive messages. This macro expands
  into a proxy for AsyncCallback."
  [])

(defn criteria
  "Constrains the target instances of a message. Options:

  {:resource
   :partition
   :partition-state
   :instance-name
   :self-excluded?
   :session-specific?}"
  [criteria]
  (doto (Criteria.)
    (.setResource (get criteria :resource "*"))
    (.setPartition (get criteria :partition "*"))
    (.setPartitionState (get criteria :partition-state "*"))
    (.setInstanceName (get criteria :instance "*"))
    (.setSelfExcluded(get criteria :self-excluded? true))))

(def evaluator (CriteriaEvaluator.))

(defn matching
  "Finds instances matching the given Criteria."
  [^HelixManager manager criteria]
  (.evaluateCriteria evaluator criteria manager))

(defn message
  "Constructs a new Message."
  [])

(defn send-message
  "Sends a Message, given a ClusterMessagingService."
  [^ClusterMessagingService service ^Message message])
