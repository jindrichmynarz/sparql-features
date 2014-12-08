(defproject sparql-features "0.1.0-SNAPSHOT"
  :description "Feature statistics with SPARQL"
  :url "http://github.com/jindrichmynarz/sparql-features"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [com.taoensso/timbre "3.1.1"]
                 [com.stuartsierra/component "0.2.2"]
                 [org.clojure/tools.cli "0.3.1"]
                 [clj-time "0.7.0"]
                 [clj-http "1.0.0"]
                 [slingshot "0.10.3"]
                 [org.clojure/data.zip "0.1.1"]
                 [org.clojure/data.csv "0.1.2"]
                 [stencil "0.3.3"]
                 [clj-yaml "0.4.0"]
                 [prismatic/schema "0.2.4"]
                 [schema-contrib "0.1.3"]]
  :main sparql-features.core
  :profiles {:uberjar {:aot :all}})
