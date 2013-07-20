(ns clj-helix.fsm
  "Supports the Helix finite state machine."
  (:import (org.apache.helix NotificationContext)
           (org.apache.helix.model Message
                                   StateModelDefinition
                                   StateModelDefinition$Builder)
           (org.apache.helix.participant.statemachine StateModel
                                                      StateModelFactory
                                                      Transition))
  (:require [alandipert.interpol8 :refer [interpolating]]
            [tailrecursion.javastar :as javastar]))

(defn validate-state-name
  "Throws if the state name isn't a valid state name."
  [state]
  (if (and (or (keyword? state)
               (string? state))
           (re-matches #"^[A-Za-z][A-Za-z0-9_]*$" (name state)))
    state
    (throw (IllegalArgumentException.
             (str "Invalid state: " (pr-str state))))))

(defn fsm-definition
  "An FSM definition is a named collection of states, describing their
  priorities, transitions, constraints, etc. It looks like: 
    
  {:name   :my-fsm
   :states {:master {:priority 1
                     :transitions :slave
                     :upper-bound 1}
            :slave  {:priority 2
                     :transitions [:master :offline]
                     :upper-bound :R}
            :offline {:transitions :slave
                      :initial? true}}})

  This function validates the structure of an FSM definition, blessing it for
  use later."
  [d]
  ; Has a name
  (assert (or (string? (:name d))
              (keyword? (:name d))))

  (let [states (:states d)]
    ; Has states
    (assert (map? states))

    ; Exactly one initial state.
    (assert (= 1 (count (filter :initial? (vals (:states d))))))

    ; Normalize transitions
    (let [states (->> states
                      (map (fn [[k v]]
                             [k (assoc v :transitions
                                       (if (sequential? (:transitions v))
                                         (:transitions v)
                                         [(:transitions v)]))]))
                      (into {}))

          ; Compute all state names
          state-names (->> (keys states)
                           (concat (mapcat :transitions (vals states)))
                           (remove nil?)
                           set)]

      ; Validate state names
      (dorun (map validate-state-name state-names))

      ; No transitions to states that aren't defined.
      (assert (= state-names (set (keys states))))

      ; Done
      (assoc d :states states))))

(defn state-model-definition
  "Takes a Clojure FSM definition and turns it into a StateModelDefinition."
  [definition]
  (let [model-name (:name definition)
        model      (:states definition)] 

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
      (.build b))))

(defn states
  "Given an FSM definition, returns all the states involved."
  [definition]
  (keys (:states definition)))

(defn initial-state
  "Given an FSM definition, returns the initial state."
  [definition]
  (when-let [state (some :initial? (:states definition))]
    (key state)))

(defn transition-name
  "A method name (string) for a state transition."
  [old new]
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
  returns the classname as a symbol. Takes an FSM definition, and a map of
  transitions like
  
  {[old-state new-state] -> (fn [partition msg ctx] ...)}"
  [fsm-def transitions]
  (let [class-name (-> fsm-def
                       :name
                       name
                       (str "_")
                       munge
                       gensym
                       str)
        ; Nothing says fun like mangling strings through three layers of
        ; languages!
        initial-state (initial-state fsm-def)
        
        states-str  (str "{"
                         (->> fsm-def
                              states
                              (map #(str "'" (name %) "'"))
                              (interpose ",")
                              (apply str))
                         "}")
        state-transitions (->> transitions
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
  [fsm-def transitions]
  (let [class-name (state-model-class fsm-def transitions)]
    ; Assign static transition functions
    (doseq [[[old new] f] transitions]
      (let [field (str (transition-name old new) "Fn")]
        (-> class-name
            (.getDeclaredField field)
            (.set nil f))))

    ; Return a constructor fn
    (eval `(fn [^String partitionId#] (new ~class-name partitionId#)))))

(defmacro state-model
  "Builds a state machine. First, takes an FSM definition. Then come a series
  of state transitions, which are lists of the old state, the new state, a
  binding form for the partition ID, Message, and NotificationContext, and
  finally a body, which will be evaluated when that particular state transition
  occurs. Bodies are closures over the local lexical scope.

  (state-model fsm-definition
    
    (:Offline :Online [part msg ctx]
     (prn :was :offline :now :online msg))

    (:Online :Offline [part msg ctx]
     (prn :offline ctx)))"
  [definition & transitions]
  (let [transitions
        (->> transitions
             (map (fn [[old new [part msg ctx] & body]]
                    `[[~old ~new]
                      (fn [~(vary-meta part assoc :tag String)
                           ~(vary-meta msg  assoc :tag Message)
                           ~(vary-meta ctx  assoc :tag NotificationContext)]
                        ~@body)]))
             (into {}))]
    `(state-model* ~definition ~transitions)))

(definterface FSM
  (definition []))

(defmacro fsm
  "Takes the same arguments as state-model, and expands into an expression
  which yields an instance of an anonymous StateModelFactory subclass, which
  generates instances of the given state model when asked.
  
  Go home Java, you're drunk."
  [fsm-def & transitions]
  ; Capture the FSM definition so it can be extracted from the factory later.
  `(let [fsm-def# ~fsm-def
         constructor# (state-model fsm-def# ~@transitions)]
     (proxy [StateModelFactory FSM] []
       (definition [] fsm-def#)

       (createNewStateModel [partition-id#]
         (constructor# partition-id#)))))
