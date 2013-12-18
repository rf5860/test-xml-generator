(ns test-file-generator.core)
(require '[clojure.java.jdbc :as j])
(use '[clojure.string :only [upper-case split replace join]])

(def ora-db {:classname "oracle.jdbc.OracleDriver"
             :subprotocol "oracle"
             :subname "thin:@localhost:1521:orcl" 
             :user "user"
             :password "password"
             })

(def sample-queries
  ["SELECT * FROM MWPTODO_STAT" "SELECT * FROM MSF010 WHERE TABLE_TYPE = 'N5'"])

(defn get-table-name
  [query]
  (nth (split query #" ") 3))

(defn convert-row-to-string
  [row]
  (join " " (for [[k v] row] (str (upper-case (replace k ":" "")) "=\"" (upper-case v) "\""))))

(defn generate-dbunit-entry
  [table-name row]
  (str "<" table-name " " (convert-row-to-string row) " />"))

(defn generate-dbunit-entries
  ([query]
     (generate-dbunit-entries (get-table-name query) (j/query ora-db [query])))
  ([table-name rows]
     (join "\n" (map #(generate-dbunit-entry table-name %1) rows))))

(defn generate-data-file
  [queries]
  (let [contents (join "\n" (map #(generate-dbunit-entries %1) queries))]
    (str "<?xml version='1.0' encoding='UTF-8'?>\n<dataset>\n" contents "\n</dataset>")))
