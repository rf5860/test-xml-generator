(ns test-file-generator.core
  (:gen-class))
(require '[clojure.java.jdbc :as j])
(use '[clojure.string :only [upper-case split join]])

(def ora-db {:classname "oracle.jdbc.OracleDriver"
             :subprotocol "oracle"
             :subname "thin:@localhost:1521:orcl" 
             :user "user"
             :password "pwd"
             })

(def sample-queries
  ["SELECT * FROM MWPTODO_STAT" "SELECT * FROM MSF010 WHERE TABLE_TYPE = 'N5'"])

(defn get-table-name
  [query]
  (nth (split query #" ") 3))

(defn convert-row-to-string
  [row]
  (join " " (for [[k v] row] (str (upper-case (clojure.string/replace k ":" "")) "=\"" (upper-case v) "\""))))

(defn generate-dbunit-entry
  [table-name row]
  (str "<" table-name " " (convert-row-to-string row) " />"))

(defn generate-data-lines
  [table-name rows]
  (join "\n" (map #(generate-dbunit-entry table-name %1) rows)))

(defn generate-dbunit-entries
  [odb query]
  (generate-data-lines (get-table-name query) (j/query odb [query])))

(defn generate-data-file
  [odb queries]
  (let [contents (join "\n" (map #(generate-dbunit-entries odb %1) queries))]
    (str "<?xml version='1.0' encoding='UTF-8'?>\n<dataset>\n" contents "\n</dataset>")))

(defn get-lines [fname]
  (with-open [r (clojure.java.io/reader fname)]
    (doall (line-seq r))))

(defn get-db-obj
  [user pwd]
  {:classname "oracle.jdbc.OracleDriver"
   :subprotocol "oracle"
   :subname "thin:@localhost:1521:orcl" 
   :user user
   :password pwd
   })

(defn -main
  [& args]
  (println (generate-data-file (get-db-obj (nth args 0) (nth args 1)) (get-lines "queries.txt"))))
