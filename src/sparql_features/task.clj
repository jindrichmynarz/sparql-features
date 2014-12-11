(ns sparql-features.task
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [sparql-features.config :refer [->Config]]
            [sparql-features.sparql :as sparql]))

(declare random-uri write-frequency)

; ----- Private functions -----

(defn- filter-satisfiable-properties
  "Reduce the list of @properties with specified @directionality
  to those that appear in the configured dataset."
  [task directionality properties]
  {:pre [(#{"in" "out"} directionality)]}
  (let [template-path ({"in" "in_property_exists"
                        "out" "out_property_exists"} directionality)]
    (filter (fn [property] (sparql/ask (:sparql-endpoint task)
                                       template-path
                                       :data {:classes (:classes task)
                                              :property property
                                              :source-graph (:source-graph task)}))
            properties)))

(defn- load-property-frequencies
  "Loads frequencies for @property with @directionality into @writer."
  [task property directionality writer]
  {:pre [(#{"in" "out"} directionality)]}
  (let [sparql-endpoint (:sparql-endpoint task)
        source-graph (:source-graph task)
        target-graph (random-uri)]
    (sparql/update-unlimited sparql-endpoint
                             (format "load_%sbound_property_resources" directionality)
                             :data {:classes (:classes task) 
                                    :property property
                                    :source-graph source-graph
                                    :target-graph target-graph}
                             :limit (:page-size task))
    (doseq [result (sparql/select-unlimited sparql-endpoint
                                            (format "%s_feature_frequencies" directionality)
                                            :data {:property property
                                                   :source-graph source-graph 
                                                   :target-graph target-graph}
                                            :limit (:page-size task)
                                            :parallel? (:parallel-execution task))]
      (write-frequency result directionality writer))
    (sparql/delete-graph sparql-endpoint target-graph)))

(defn- random-uri
  "Generates a random URI"
  []
  (str "http://example.com/" (java.util.UUID/randomUUID)))

(defn- warn-property-removed
  "Prints a warning for each removed property from @removed-properties
  with the given @directionality."
  [directionality removed-properties]
  {:pre [(#{"in" "out"} directionality)]}
  (let [dir-str ({"in" "Inbound"
                  "out" "Outbound"} directionality)]
    (doseq [property removed-properties]
      (println (format "%s property <%s> wasn't satisfiable, so was removed." dir-str property)))))

(defn- write-frequency
  "Write @frequency in TSV for given @property and @resource connected with @directionality
  to the @writer."
  [{:keys [frequency property resource]} directionality writer]
  (write-csv writer
             [[resource
               property
               directionality
               frequency]]
             :separator \tab))

; ----- Records -----

(defrecord Task []
  component/Lifecycle
  (start [{{{{:keys [inbound outbound]} :properties
             :as task-config} :task} :config
           :keys [sparql-endpoint]
           :as task}]
    (let [task' (merge task task-config)
          inbound-properties (filter-satisfiable-properties task' "in" inbound)
          outbound-properties (filter-satisfiable-properties task' "out" outbound)]
      (doseq [[directionality
               removed-properties] {"in" (clojure.set/difference (set inbound) (set inbound-properties))
                                    "out" (clojure.set/difference (set outbound) (set outbound-properties))}]
        (warn-property-removed directionality removed-properties)) 
    (assoc task' :properties {:inbound inbound-properties
                              :outbound outbound-properties})))
  (stop [task] task))

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
  [task output-file]
  (with-open [output (io/writer output-file)]
    ; Write TSV header
    (write-csv output [["resource" "property" "directionality" "frequency"]] :separator \tab)
    (doseq [directionality ["in" "out"]]
      (doseq [property (get-in task [:properties ({"in" :inbound
                                                   "out" :outbound} directionality)])]
        (load-property-frequencies task property directionality output)))))
