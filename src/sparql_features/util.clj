(ns sparql-features.util
  (:import [java.security MessageDigest]))

; ----- Public functions -----

(defn exit
  "Exit with @status and message @msg.
  @status 0 is OK, @status 1 indicates error."
  [^Integer status
   ^String msg]
  {:pre [(#{0 1} status)]}
  (println msg)
  (System/exit status))

(defn sha1
  "Computes SHA1 hash from @string."
  [^String string]
  (let [digest (.digest (MessageDigest/getInstance "SHA1") (.getBytes string))]
    ;; Stolen from <https://gist.github.com/kisom/1698245#file-sha256-clj-L19>
    (clojure.string/join (map #(format "%02x" (bit-and % 0xff)) digest))))
