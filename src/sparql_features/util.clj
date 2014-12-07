(ns sparql-features.util)

; ----- Public functions -----

(defn exit
  "Exit with @status and message @msg.
  @status 0 is OK, @status 1 indicates error."
  [^Integer status
   ^String msg]
  {:pre [(#{0 1} status)]}
  (println msg)
  (System/exit status))
