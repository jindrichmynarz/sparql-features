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

(declare ask execute-query execute-sparql execute-update render-sparql try-times*)

; ----- Macros -----

(defmacro try-times
  "Executes @body. If an exception is thrown, will retry. At most @times retries
  are done. If still some exception is thrown it is bubbled upwards in the call chain.
  Adapted from <http://stackoverflow.com/a/1879961/385505>."
  [times & body]
  `(try-times* ~times (fn [] ~@body)))

; ----- Private functions -----

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

(defn- try-times*
  "Try @body for number of @times. If number of retries exceeds @times,
  then exception is raised. Each unsuccessful try is followed by sleep,
  which increase in length in subsequent tries."
  [times body]
  (loop [n times]
    (if-let [result (try [(body)]
                         (catch Exception ex
                           (if (zero? n)
                             (throw ex)
                             (Thread/sleep (-> (- times n)
                                               (* 20000)
                                               (+ 10000))))))]
      (result 0)
      (recur (dec n)))))

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

(defn execute-query
  "Render @template-path using @data and execute the resulting SPARQL query."
  [sparql-endpoint template-path & {:keys [data]}]
  (try-times (inc (:retry-count sparql-endpoint)) ; Try once, then retry for the specified number of times.
             (execute-sparql sparql-endpoint
                             :endpoint-url (get-in sparql-endpoint [:endpoints :query-url])
                             :method :GET
                             :request-string (render-sparql template-path :data data))))

(defn execute-update
  "Render @template-path using @data and execute the resulting SPARQL update request."
  [sparql-endpoint template-path & {:keys [data]}]
  (execute-sparql sparql-endpoint
                  :endpoint-url (get-in sparql-endpoint [:endpoints :update-url])
                  :method :POST
                  :request-string (render-sparql template-path
                                                 :data (assoc data :virtuoso (:virtuoso? sparql-endpoint)))))

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
  "Lazily stream pages of SPARQL SELECT query results
  by executing paged query from @template-path."
  [sparql-endpoint template-path & {:keys [data]}]
  (let [page-size (get-in sparql-endpoint [:page-size :query])
        map-fn (if (:parallel-execution sparql-endpoint) pmap map)
        select-fn (fn [offset]
                    (select sparql-endpoint template-path :data (assoc data
                                                                       :limit page-size
                                                                       :offset offset)))]
    (->> (iterate (partial + page-size) 0)
         (map-fn select-fn)
         (take-while seq)
         lazy-cat')))

(defn update-unlimited
  "Eagerly execute SPARQL Update operation split into @limit-sized parts."
  ; Relies on Virtuoso-specific responses in order to stop iteration.
  ; Reason why:
  ; <http://answers.semanticweb.com/questions/29420/stopping-condition-for-paged-sparql-update-operations/29422>
  ; An alternative is to provide explicit @max-count of maximum number of bindings to update.
  [sparql-endpoint template-path & {:keys [data max-count]}]
  (let [page-size (get-in sparql-endpoint [:page-size :update])
        message-regex (re-pattern #"(\d+)( \(or less\))? triples")
        update-fn (fn [offset] {:offset offset 
                                :result (execute-update sparql-endpoint
                                                        template-path
                                                        :data (merge {:limit page-size
                                                                      :offset offset} data))})
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
    (dorun (->> (iterate (partial + page-size) 0)
                (map update-fn)
                (take-while continue?)))))

; ----- Records -----

(defrecord SparqlEndpoint []
  component/Lifecycle
  (start [{{{:keys [password query-url update-url username]
             :as sparql-config} :sparql-endpoint} :config
           :as sparql-endpoint}]
    (let [authentication [username password]
          new-endpoint (merge (assoc sparql-endpoint
                                     :authentication authentication
                                     :endpoints {:query-url query-url
                                                 :update-url update-url})
                              sparql-config)
          server-header (-> query-url
                            client/head
                            (get-in [:headers :Server]))
          virtuoso? (when-not (nil? server-header)
                      (not= (.indexOf (clojure.string/lower-case server-header) "virtuoso") -1))]
      (assert (not-any? nil? authentication)
              "Password and username are missing from the configuration!")
      (ping-endpoint new-endpoint)
      (assoc new-endpoint :virtuoso? virtuoso?)))
  (stop [sparql-endpoint] sparql-endpoint))
