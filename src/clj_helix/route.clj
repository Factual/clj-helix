(ns clj-helix.route
  "Helps a local manager figure out where to send messages to."
  (:use [clj-helix.admin :only [instance-config->map]])
  (:import (org.apache.helix.spectator RoutingTableProvider)
           (org.apache.helix HelixManager)))

(defn router!
  "Creates a new router atop a HelixManager. Note that this modifies the
  HelixManager; you should store the return value instead of re-invoking it."
  [^HelixManager manager]
  (let [r (RoutingTableProvider.)]
    (.addExternalViewChangeListener manager r)
    (.addConfigChangeListener manager r)
    r))

(defn instances
  "Finds instances which are in a particular state."
  ([^RoutingTableProvider router resource partition state]
   (->> (.getInstances router (name resource) partition (name state))
        (map instance-config->map)))

  ([^RoutingTableProvider router resource state]
   (->> (.getInstances router (name resource) (name state))
        (map instance-config->map))))
