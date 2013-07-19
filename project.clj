(defproject clj-helix "0.1.0-SNAPSHOT"
  :description "Clojure bindings for Apache Helix"
  :url "http://github.com/aphyr/clj-helix"
  :aot [clj-helix.state-model]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.helix/helix-core "0.6.1-incubating"]
                 [shady "0.1.1"]])
