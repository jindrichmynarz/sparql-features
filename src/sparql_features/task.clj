(ns sparql-features.task
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [sparql-features.config :refer [->Config]]
            [sparql-features.sparql :as sparql]))

(declare serialize-feature-frequency tab-join)

; ----- Private functions -----

(defn- load-features
  "Load features to @target-graph using SPARQL Update from @template-path."
  [task template-path target-graph]
  (sparql/update-unlimited (:sparql-endpoint task)
                           template-path
                           :data (assoc task
                                        :target-graph target-graph)
                           :limit (:page-size task)))

(defn- load-feature-frequencies
  "Load frequencies of features using SPARQL SELECT query from @template-path.
  Results are serialized into @output-file in TSV."
  [task template-path directionality writer]
  {:pre [(#{"in" "out"} directionality)]}
  (doseq [result (sparql/select-unlimited (:sparql-endpoint task)
                                          template-path
                                          :data task 
                                          :limit (:page-size task)
                                          :parallel? (:parallel-execution task))]
    (write-csv writer
               [[(:resource result)
                 (:property result)
                 directionality
                 (:frequency result)]]
               :separator \tab)))

(defn- random-uri
  "Generates a random URI"
  []
  (str "http://example.com/" (java.util.UUID/randomUUID)))

; ----- Records -----

(defrecord Task []
  component/Lifecycle
  (start [{{task-config :task} :config
           :as task}]
    (merge task
           task-config
           {:in-features-graph (random-uri)
            :out-features-graph (random-uri)}))
  (stop [{:keys [in-features-graph out-features-graph sparql-endpoint]
          :as task}]
    (let [graphs [in-features-graph out-features-graph]]
      (dorun (map (partial sparql/delete-graph sparql-endpoint) graphs))
      (assert (not-any? (partial sparql/graph-exists? sparql-endpoint) graphs)
              (apply format "Temporary graphs <%s> and <%s> weren't properly deleted!" graphs))
      task)))

(defn load-system
  "Load a system with task from configuration in @config-file-path."
  [config-file-path]
  (try
    (component/start
      (component/system-map :config (->Config config-file-path)
                            :sparql-endpoint (component/using (sparql/->SparqlEndpoint) [:config])
                            :task (component/using (->Task) [:config :sparql-endpoint])))
    (catch Exception ex
      (timbre/error (.getCause ex))
      (throw ex))))

; ----- Public functions -----

(defn get-feature-stats
  "Get feature statistics based on @task configuration.
  Output TSV into @output-file."
  [{:keys [in-features-graph out-features-graph]
    :as task} output-file]
  (load-features task "load_in_features" in-features-graph) 
  (load-features task "load_out_features" out-features-graph)
  (with-open [output (io/writer output-file)]
    ; Write TSV header
    (write-csv output [["directionality" "resource" "property" "frequency"]] :separator \tab)
    (load-feature-frequencies task "in_features_frequencies" "in" output)
    (load-feature-frequencies task "out_features_frequencies" "out" output)))
