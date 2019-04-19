(defproject pipeline "0.0.1"
  :description "Clojure Pipeline Example"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/core.async "0.4.490"]
                 [org.clojure/data.json "0.2.6"]
                 [org.clojure/spec.alpha "0.2.176"]]
  :main ^:skip-aot pipeline.core)
