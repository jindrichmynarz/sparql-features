(ns sparql-features.features
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.data.csv :refer [write-csv]]
            [clojure.java.io :as io]
            [sparql-features.config :refer [->Config]]
            [sparql-features.util :refer [sha1]]
            [sparql-features.sparql :as sparql]))

(declare random-uri write-frequency)

; ----- Private functions -----

(defn- filter-satisfiable-properties
  "Reduce the list of @properties with specified @directionality
  to those that appear in the configured dataset."
  [features directionality properties]
  {:pre [(#{"in" "out"} directionality)]}
  (let [template-path ({"in" "in_property_exists"
                        "out" "out_property_exists"} directionality)
        {:keys [classes source-graph sparql-endpoint]} features]
    (filter (fn [property] (sparql/ask sparql-endpoint 
                                       template-path
                                       :data {:classes classes
                                              :property property
                                              :source-graph source-graph}))
            properties)))

(defn- load-property-frequencies
  "Loads frequencies for @property with @directionality into @writer."
  [features property directionality writer]
  {:pre [(#{"in" "out"} directionality)]}
  (let [{:keys [classes page-size parallel-execution source-graph sparql-endpoint]} features
        target-graph (random-uri)]
    (try (sparql/update-unlimited sparql-endpoint
                                  (format "load_%sbound_property_resources" directionality)
                                  :data {:classes classes 
                                         :property property
                                         :source-graph source-graph
                                         :target-graph target-graph}
                                  :limit page-size)
         (doseq [result (sparql/select-unlimited sparql-endpoint
                                                 (format "%s_feature_frequencies" directionality)
                                                 :data {:property property
                                                        :source-graph source-graph 
                                                        :target-graph target-graph}
                                                 :limit page-size 
                                                 :parallel? parallel-execution)]
           (write-frequency result directionality writer))
         ; Ensure that the temporary graph is deleted even if exception is raised.
         (finally (sparql/delete-graph sparql-endpoint target-graph)))))

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

(defrecord Features []
  component/Lifecycle
  (start [{{{{:keys [inbound outbound]} :properties
             :as features-config} :features} :config
           :keys [sparql-endpoint]
           :as features}]
    (let [features' (merge features features-config)
          inbound-properties (filter-satisfiable-properties features' "in" inbound)
          outbound-properties (filter-satisfiable-properties features' "out" outbound)]
      (doseq [[directionality
               removed-properties] {"in" (clojure.set/difference (set inbound) (set inbound-properties))
                                    "out" (clojure.set/difference (set outbound) (set outbound-properties))}]
        (warn-property-removed directionality removed-properties)) 
      (assoc features' :properties {:inbound inbound-properties
                                    :outbound outbound-properties})))
  (stop [features] features))

(defn load-system
  "Load a system with feature definition from configuration in @config-file-path."
  [config-file-path]
  (try
    (component/start
      (component/system-map :config (->Config config-file-path)
                            :sparql-endpoint (component/using (sparql/->SparqlEndpoint) [:config])
                            :features (component/using (->Features) [:config :sparql-endpoint])))
    (catch Exception ex
      (timbre/error (.getCause ex))
      (throw ex))))

; ----- Public functions -----

(defn get-features
  "Saves resource features into separate TSV files."
  [{:keys [classes properties source-graph sparql-endpoint]} directory]
  (let [dir (io/as-file directory)]
    (when-not (.exists dir) (.mkdir dir)))
  (doseq [item (partition-by :resource (sparql/select-unlimited sparql-endpoint
                                                                "load_features"
                                                                :data {:classes classes
                                                                       :properties properties
                                                                       :source-graph source-graph}))
          :let [filename (sha1 (-> item first :resource))]]
    (with-open [writer (io/writer (format "%s/%s.tsv" directory filename))]
      (doseq [{:keys [resource property object directionality]} item]
        (write-csv writer
                   [[resource
                     property
                     object
                     directionality]]
                   :separator \tab)))))

(defn get-feature-stats
  "Get feature statistics based on @features configuration.
  Output TSV into @output-file."
  [features output-file]
  (with-open [output (io/writer output-file)]
    ; Write TSV header
    (write-csv output [["resource" "property" "directionality" "frequency"]] :separator \tab)
    (doseq [directionality ["in" "out"]]
      (doseq [property (get-in features [:properties ({"in" :inbound
                                                       "out" :outbound} directionality)])]
        (load-property-frequencies features property directionality output)))))

(comment
  (def system (load-system "local-config.yaml"))
  (def system (load-system "sisinf-config.yaml"))
  (def features (:features system))
  (load-features features "local-features.tsv")
  (load-features features "dbpedia-features.tsv")
  )
