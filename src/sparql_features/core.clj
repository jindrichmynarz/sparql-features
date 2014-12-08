(ns sparql-features.core
  (:gen-class)
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clojure.string :refer [join]]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :refer [as-file]]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [sparql-features.util :refer [exit]]
            [sparql-features.task :as task]))

; ----- Private vars -----

(def ^:private
  cli-options
  [["-c" "--config CONFIG" "Path to configuration file in YAML"
    :validate [#(.exists (as-file %)) "The configuration file doesn't exist!"]]
   ["-l" "--log" "Use this switch to turn on logging"]
   ["-o" "--output TSV" "Path to the output TSV file (default: standard output)"
    :default *out*]
   ["-h" "--help" "Display this help message"]])

(def ^:private
  log-file
  "log/sparql_features.log")

; ----- Private functions -----

(defn- error-msg
  [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))

(defn- init-logger
  "Initialize logger. If @debug? is true then the log level is set to debug."
  [debug?]
  (let [log-directory (as-file "log")]
    (timbre/set-level! (if debug? :debug :info))
    (when debug?
      (when-not (.exists log-directory) (.mkdir log-directory))
      (println (format "Logging into %s." log-file))
      (timbre/set-config! [:appenders :standard-out :enabled?] false)
      (timbre/set-config! [:appenders :spit :enabled?] true)
      (timbre/set-config! [:shared-appender-config :spit-filename] log-file))))

(defn- usage
  [options-summary]
  (->> ["Load feature statistics from SPARQL endpoints"
        ""
        "Usage: java -jar sparql-features.jar [options]"
        ""
        "Options:"
        options-summary]
       (join \newline)))

; ----- Public functions -----

(defn -main
  [& args]
  (let [{{:keys [config help log output]
          :as options} :options
         :keys [errors summary]} (parse-opts args cli-options)]
    (cond (or (empty? options) help) (exit 0 (usage summary))
          errors (exit 1 (error-msg errors))
          :else (let [_ (init-logger log)
                      start-time (System/currentTimeMillis)
                      system (task/load-system config)]
                  (.addShutdownHook (Runtime/getRuntime) (Thread. (fn [] (println "Shutting down...")
                                                                         (component/stop system)
                                                                         (shutdown-agents))))
                  (println "Generating feature statistics...")
                  (task/get-feature-stats (:task system) output)
                  (println (format "Feature statistics generation took %s seconds."
                                   (t/in-seconds (t/interval (c/from-long start-time)
                                                             (c/from-long (System/currentTimeMillis))))))))))
