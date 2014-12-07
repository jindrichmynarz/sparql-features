(ns sparql-features.sparql
  (:require [taoensso.timbre :as timbre]
            [com.stuartsierra.component :as component]
            [clj-http.client :as client]
            [clojure.xml :as xml]
            [clojure.zip :as zip]
            [clojure.data.zip.xml :as zip-xml]
            [stencil.core :refer [render-file]]
            [stencil.loader :refer [set-cache]]
            [slingshot.slingshot :refer [throw+ try+]]
            [sparql-features.config :refer [->Config]]))

(declare ask execute-sparql render-sparql)

; ----- Private functions -----

(defn- execute-query
  "Render @template-path using @data and execute the resulting SPARQL query."
  [sparql-endpoint template-path & {:keys [data]}]
  (execute-sparql sparql-endpoint
                  :endpoint-url (get-in sparql-endpoint [:endpoints :query-url])
                  :method :GET
                  :request-string (render-sparql template-path :data data)))

(defn- execute-sparql
  "Execute SPARQL @query-string on @endpoint-url of @sparql-endpoint using @method."
  [sparql-endpoint & {:keys [accept endpoint-url method request-string]
                      :or {accept "application/sparql-results+xml"}}]
  (let [[method-fn params-key request-key] (case method
                                             :GET [client/get :query-params "query"]
                                             ; Fuseki requires form-encoded params
                                             :POST [client/post :form-params "update"])
        params (merge {params-key {request-key request-string}
                       :accept accept
                       :throw-entire-message? true}
                      (when (= method :POST) {:digest-auth (:authentication sparql-endpoint)}))]
    (try+ (timbre/debug (str "Executing SPARQL\n" request-string))
          (:body (method-fn endpoint-url params))
          (catch [:status 400] {:keys [body]}
            (timbre/error body)
            (throw+)))))

(defn- execute-update
  "Render @template-path using @data and execute the resulting SPARQL update request."
  [sparql-endpoint template-path & {:keys [data]}]
  (execute-sparql sparql-endpoint
                  :endpoint-url (get-in sparql-endpoint [:endpoints :update-url])
                  :method :POST
                  :request-string (render-sparql template-path :data data)))

(defn- lazy-cat'
  "Lazily concatenates lazy sequence of sequences @colls.
  Taken from <http://stackoverflow.com/a/26595111/385505>."
  [colls]
  (lazy-seq
    (if (seq colls)
      (concat (first colls) (lazy-cat' (next colls))))))

(defn- ping-endpoint
  "Send a simple SPARQL ASK query to @sparql-endpoint to test if it's available."
  [sparql-endpoint]
  (assert (ask sparql-endpoint "ping_endpoint")
          (format "SPARQL endpoint %s isn't available!" (get-in sparql-endpoint [:endpoints :query-url]))))

(defn- render-sparql
  "Render SPARQL from Mustache template on @template-path using @data."
  [template-path & {:keys [data]}]
  (render-file (str "templates/" template-path ".mustache") data))

(defn- xml->zipper
  "Take XML string @s, parse it, and return XML zipper"
  [s]
  (->> s
       .getBytes
       java.io.ByteArrayInputStream.
       xml/parse
       zip/xml-zip))

; ----- Public functions -----

(defn ask
  "Render @template-path using @data and execute the resulting SPARQL ASK query."
  [sparql-endpoint template-path & {:keys [data]}]
  (let [results (-> (execute-query sparql-endpoint template-path :data data) 
                    xml->zipper
                    (zip-xml/xml1-> :boolean zip-xml/text))]
    (boolean (Boolean/valueOf results))))

(defn delete-graph
  "Delete graph @graph-uri."
  [sparql-endpoint graph-uri]
  (execute-update sparql-endpoint "clear_graph" :data {:graph-uri graph-uri}))

(defn graph-exists?
  "Test if named graph @graph-uri exists in @sparql-endpoint."
  [sparql-endpoint graph-uri]
  (ask sparql-endpoint "graph_exists" :data {:graph-uri graph-uri}))

(defn select
  "Execute SPARQL SELECT query rendered from @template-path with @data.
  Returns empty sequence when query has no results."
  [sparql-endpoint template-path & {:keys [data]}]
  (let [results (xml->zipper (execute-query sparql-endpoint template-path :data data))
        sparql-variables (map keyword (zip-xml/xml-> results :head :variable (zip-xml/attr :name)))
        sparql-results (zip-xml/xml-> results :results :result)
        get-bindings (comp (partial zipmap sparql-variables) #(zip-xml/xml-> % :binding zip-xml/text))]
    (map get-bindings sparql-results)))                           

(defn select-unlimited
  "Lazily stream @limit-sized pages of SPARQL SELECT query
  results by executing paged query from @template-path."
  [sparql-endpoint template-path & {:keys [data limit parallel?]
                                    :or {limit 5000}}]
  (let [map-fn (if parallel? pmap map)
        select-fn (fn [offset]
                    (select sparql-endpoint template-path :data (assoc data
                                                                       :limit limit
                                                                       :offset offset)))]
    (->> (iterate (partial + limit) 0)
         (map-fn select-fn)
         (take-while seq)
         lazy-cat')))

(defn update-unlimited
  "Eagerly execute SPARQL Update operation split into @limit-sized parts."
  ; Relies on Virtuoso-specific responses in order to stop iteration.
  ; Reason why:
  ; <http://answers.semanticweb.com/questions/29420/stopping-condition-for-paged-sparql-update-operations/29422>
  ; An alternative is to provide explicit @max-count of maximum number of bindings to update.
  [sparql-endpoint template-path & {:keys [data limit max-count]
                                    :or {limit 5000}}]
  (let [message-regex (re-pattern #"(\d+)( \(or less\))? triples")
        update-fn (fn [offset] {:offset offset 
                                :result (execute-update sparql-endpoint
                                               template-path
                                               :data (merge {:limit limit :offset offset} data))})
        triples-changed (comp (fn [number-like]
                                (Integer/parseInt number-like))
                              second
                              (fn [message]
                                (re-find message-regex message))
                              first
                              (fn [zipper]
                                (zip-xml/xml-> zipper :results :result :binding :literal zip-xml/text)) 
                              xml->zipper)
        continue? (if-not (nil? max-count)
                    (fn [response] (< max-count (:offset response)))
                    (fn [response] (-> response :result triples-changed zero? not)))]
    (dorun (->> (iterate (partial + limit) 0)
                (map update-fn)
                (take-while continue?)))))

; ----- Records -----

(defrecord SparqlEndpoint []
  component/Lifecycle
  (start [{{{:keys [password query-url update-url username]} :sparql-endpoint} :config
           :as sparql-endpoint}]
    (let [authentication [username password]
          new-endpoint (assoc sparql-endpoint
                              :authentication authentication
                              :endpoints {:query-url query-url
                                          :update-url update-url})]
      (assert (not-any? nil? authentication)
              "Password and username are missing from the configuration!")
      (ping-endpoint new-endpoint)
      new-endpoint))
  (stop [sparql-endpoint] sparql-endpoint))
