(ns clj-helix.state-model
  "The Helix state model uses annotations to figure out
  which methods on the state model class to call, when a state transition
  occurs. We need to generate a class, given method bodies, which are tagged
  with the appropriate state transition annotations. To do this, we exploit
  gen-class."
  (:import (org.apache.helix.participant.statemachine StateModel
                                                      Transition))
  (:require [shady.gen-class :as shady]))

(defmacro state-model-class
  "Takes a series of lists, each composed of an initial state, a new state, and
  any number of expressions. The expressions will be wrapped in a method body,
  and invoked when the state model undergoes that particular transition. Method
  bodies are lexical closures over the local scope.
  
  (state-model-class
    (offline online
             (prn :online))

    (online offline
           (prn :offline)))
  
  We do this by expanding into a series of (fn [] ...) definitions for each
  state transition body. Then we create a new namespace with a name like
  helix-state-model-14, and intern the fns into that namespace. Finally, we
  gen-class the namespace with the appropriate annotations."
  [& forms]
  (let [; Separate state transitions and function bodies
        transitions (map (partial take 2) forms)
        fn-bodies (map (partial drop 2) forms)
        ; Generate function names like offline-to-online
        fn-names  (map (fn [[old-state new-state]]
                         (-> (str old-state "-to-" new-state)
                             munge
                             symbol))
                         transitions)
        ; Generate anonymous fn expressions around bodies.
        fn-exprs  (map (partial concat '(fn [])) fn-bodies)
        
        ; Choose a name for this model.
        model-sym (gensym "helix-state-model-")

        ; Fully qualified class name
        class-sym (symbol (munge (str "clj-helix.state-model." model-sym)))
        class-sym 'com.aphyr.Foo
        
        ; Compute method specs for gen-class
        method-specs (mapv (fn [method-name [state state']]
                            [(with-meta method-name
                                        `{Transition {:from ~(name state)
                                                      :to ~(name state')}})
                             [] 'void])
                          fn-names transitions)]

  ; With that out of the way, let's expand into code!
  `(let [; First, define anonymous fns for each transition.
         ~@(interleave fn-names fn-exprs)

         ; Then, create a new namespace to hold these fns
         ns# (create-ns '~model-sym)]
           
     ; And intern the functions into the namespace
     ~@(for [fn-name fn-names]
         `(intern '~model-sym '~fn-name ~fn-name))

     ; Generate a class from the ns
     (binding [*compile-files* true]
       (gen-class :name    ~class-sym
                  :extends StateModel
                  :methods ~method-specs
                  :prefix ""
                  :impl-ns '~model-sym))
     '~class-sym)))

(prn (state-model-class (on off "off") (off on "on")))
(prn (new com.aphyr.Foo))
