(defproject factual/clj-helix "0.1.0"
  :description "Clojure bindings for Apache Helix"
  :url "http://github.com/factual/clj-helix"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.helix/helix-core "0.6.1-incubating"]
                 [tailrecursion/javastar "1.1.6"]]
  :java-source-paths ["src/clj_helix/"])
