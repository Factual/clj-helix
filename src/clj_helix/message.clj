(ns clj-helix.message
  (:import (org.apache.helix ClusterMessagingService
                             Criteria)
           (org.apache.helix.model Message)
           (org.apache.helix.messaging AsyncCallback)))

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
  (let [c (Criteria.)]
    (doseq [[k v] criteria]
      (case k
        :resource          (.setResource c v)
        :partition         (.setPartition c v)
        :partition-state   (.setPartitionState c v)
        :instance-name     (.setInstanceName c v)
        :self-excluded?    (.setSelfExcluded c v)
        :session-specific? (.setSessionSpecific c v)))
    c))

(defn message
  "Constructs a new Message."
  [])

(defn send-message
  "Sends a Message, given a ClusterMessagingService."
  [^ClusterMessagingService service ^Message message])
