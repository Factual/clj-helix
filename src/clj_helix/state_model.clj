(ns clj-helix.state-model
  "The Helix state model uses annotations to figure out
  which methods on the state model class to call, when a state transition
  occurs. We need to generate a class, given method bodies, which are tagged
  with the appropriate state transition annotations. To do this, we exploit
  gen-class."
  (:import (org.apache.helix NotificationContext)
           (org.apache.helix.model Message)
           (org.apache.helix.participant.statemachine StateModel
                                                      StateModelFactory
                                                      Transition))
  (:require [alandipert.interpol8 :refer [interpolating]]
            [clojure.core.cache :as cache]
            [tailrecursion.javastar :as javastar]
            clojure.pprint))

(defn validate-state
  "Throws if the state isn't a valid state name."
  [state]
  (if (and (or (keyword? state)
               (string? state))
           (re-matches #"^[A-Za-z][A-Za-z0-9_]*$" (name state)))
    state
    (throw (IllegalArgumentException.
             (str "Invalid state: " (pr-str state))))))

(defn transition-name
  "A method name (string) for a state transition."
  [old new]
  (validate-state old)
  (validate-state new)
  (str "onBecome" (name new) "From" (name old)))

(defn state-transition-code
  "Given a state transition from old-state to new-state, and a clojure function
  to invoke, returns a java static variable to hold the IFn, and a method to be
  invoked on that state transition."
  [[[old new] f]]
  (let [method-name (transition-name old new)]
    (interpolating
      "public static IFn #{method-name}Fn;

      @Transition(from = \"#{old}\", to = \"#{new}\")
      public Object #{method-name}(Message m, NotificationContext c) {
        return #{method-name}Fn.invoke(partitionId, m, c);
      }")))

(defn state-model-class
  "Generates code for a StateModel subclass, compiles and loads that class, and
  returns the classname as a symbol.
  
  :states         a list of all states
  :initial-state  the first state
  :transitions    a map of [old-state new-state] -> (fn [partition msg ctx])"
  [opts]
  (let [class-name (str (gensym "state_model_"))
        ; Nothing says fun like mangling strings through three layers of
        ; languages!
        initial-state (validate-state (name (:initial-state opts)))
        
        states-str  (str "{"
                         (->> opts
                              :states
                              (map validate-state)
                              (map #(str "'" (name %) "'"))
                              (interpose ",")
                              (apply str))
                         "}")
        state-transitions (->> (:transitions opts)
                               (map state-transition-code)
                               (interpose "\n\n")
                               (apply str))

        class-body (interpolating
"package clj_helix.state_model;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.*;
import org.apache.helix.NotificationContext;
import clojure.lang.IFn;

@StateModelInfo(states = \"#{states-str}\", initialState = \"#{initial-state}\")
public class #{class-name} extends StateModel {
  public final String partitionId;

  // Constructor
  public #{class-name}(final String partitionId) {
    this.partitionId = partitionId;
  }

  #{state-transitions}
}")]
    (javastar/compile-java (str "clj_helix.state_model." class-name)
                           class-body)))

(defn state-model*
  "Compiles and loads a new class for the given state model, returning a
  function which takes a partition ID and returns a new instance of a
  StateModel subclass."
  [opts]
  (let [class-name (state-model-class opts)]
    ; Assign static transition functions
    (doseq [[[old new] f] (:transitions opts)]
      (let [field (str (transition-name old new) "Fn")]
        (-> class-name
            (.getDeclaredField field)
            (.set nil f))))

    ; Return a constructor fn
    (eval `(fn [^String partitionId#] (new ~class-name partitionId#)))))

(defmacro state-model
  "Builds a state machine. First, takes a list of possible states, with the
  initial state first. Then come a series of state transitions, which are lists
  of the old state, the new state, a binding form for the partition ID,
  Message, and NotificationContext, and finally a body, which will be evaluated
  when that particular state transition occurs. Bodies are closures over the
  local lexical scope.

  (state-model
    [:Offline :Online]
    
    (:Offline :Online [part msg ctx]
     (prn :was :offline :now :online msg))

    (:Online :Offline [part msg ctx]
     (prn :offline ctx)))"
  [[initial-state & more :as states] & transitions]
  (let [transitions
        (->> transitions
             (map (fn [[old new [part msg ctx] & body]]
                    `[[~old ~new]
                      (fn [~(vary-meta part assoc :tag String)
                           ~(vary-meta msg  assoc :tag Message)
                           ~(vary-meta ctx  assoc :tag NotificationContext)]
                        ~@body)]))
             (into {}))]
    `(state-model* {:initial-state ~initial-state
                    :states ~states
                    :transitions ~transitions})))

(defmacro state-model-factory
  "Takes the same arguments as state-model, and expands into an expression
  which yields an instance of an anonymous StateModelFactory subclass, which
  generates instances of the given state model when asked.
  
  Go home Java, you're drunk."
  [& forms]
  `(let [constructor# (state-model ~@forms)]
     (proxy [StateModelFactory] []
       (createNewStateModel [partition-id#]
         (constructor# partition-id#)))))
