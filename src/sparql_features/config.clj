(ns sparql-features.config
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clj-yaml.core :as yaml]
            [clojure.java.io :refer [as-file resource]]
            [schema.core :as s]
            [schema-contrib.core :as sc]
            [sparql-features.util :refer [exit]]))

; ----- Public vars -----

(def ConfigSchema
  {:sparql-endpoint {:query-url sc/URI
                     :update-url sc/URI
                     :username s/Str
                     :password s/Str}
   :task {:source-graph sc/URI
          (s/optional-key :page-size) (s/both s/Int (s/pred pos? 'pos?))
          (s/optional-key :parallel-execution) s/Bool
          :classes [sc/URI]
          :properties {:inbound [sc/URI]
                       :outbound [sc/URI]}}})

; ----- Private functions -----

(defn- deep-merge
  "Deep merge maps. Stolen from:
  <https://github.com/clojure-cookbook/clojure-cookbook/blob/master/04_local-io/4-15_edn-config.asciidoc>."
  [& maps]
  (if (every? map? maps)
    (apply merge-with deep-merge maps)
    (last maps)))

; ----- Records -----

(defrecord Config [config-file-path]
  component/Lifecycle
  (start [config] (let [load-fn (comp yaml/parse-string slurp)
                        default-config (load-fn (resource "config.yaml"))
                        config (load-fn (as-file config-file-path))]
                    (try (s/validate ConfigSchema config)
                         (catch Exception ex
                           (exit 1 (format "Invalid config: \n%s" (.getMessage ex)))))
                    ; Merge default config into the provided one.
                    (reduce deep-merge [default-config config])))
  (stop [config] config))
