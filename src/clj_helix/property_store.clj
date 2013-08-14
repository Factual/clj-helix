(ns clj-helix.property-store
  "Stores small values in Helix's consistent storage system.
  
  No idea how any of this API is supposed to work, and I can't get it to do
  anything."
  (:refer-clojure :exclude [get swap!])
  (:import (org.apache.helix.store.zk ZkHelixPropertyStore
                                      ZNRecord)
           (org.apache.helix AccessOption)
           (org.apache.zookeeper.data Stat)
           (org.I0Itec.zkclient DataUpdater)))

(defn ^AccessOption options
  "Converts a keyword to an AccessOption constant. Options are:
  
  :ephemeral
  :ephemeral-sequential
  :persistent
  :persistent-sequential
  :throw-exception-if-not-exist"
  [kw]
  (case kw
    :ephemeral                    AccessOption/EPHEMERAL
    :ephemeral-sequential         AccessOption/EPHEMERAL_SEQUENTIAL
    :persistent                   AccessOption/PERSISTENT
    :persistent-sequential        AccessOption/PERSISTENT_SEQUENTIAL
    :throw-exception-if-not-exist AccessOption/THROW_EXCEPTION_IFNOTEXIST))

(defn ^Stat stat
  "Constructs a new ZK stat object"
  []
  (Stat.))

(defn get*
  "Gets a value from the property store. Returns [value, stat]."
  [^ZkHelixPropertyStore store path opts]
  (let [stat (stat)]
    [(.get store path stat (options opts))
     stat]))

(defn get
  "Like get*, but returns only the value, discarding the stat."
  [store path opts]
  (first (get* store path opts)))

(defn swap!
  "Atomically updates a path, setting its value to (f current-value args).
  Returns the new value."
  [^ZkHelixPropertyStore store path opts f & args]
  (let [x (atom nil)]
    (if (.update store path (reify DataUpdater
                              (update [this value]
                                (prn "Updating" value)
                                (prn "Plan to return" (reset! x (apply f value args)))
                                (reset! x (apply f value args))))
                 (options opts))
      @x
      (throw (RuntimeException. (str "update of " path "failed"))))))

(defn cas!
  "Atomically compares and sets a path, given an old and new value."
  [store path options old-val new-val]
  (swap! store path options (fn [cur-val]
                              (if (= old-val cur-val)
                                new-val
                                cur-val))))
