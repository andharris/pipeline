(ns pipeline.core
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]
            [clojure.core.async :as a]
            [clojure.spec.alpha :as spec]))

;; Parse json file
(defn parse-json-file-lazy [file]
  (map #(json/read-str % :key-fn keyword)
       (line-seq (io/reader file))))

;; Schema defintion and validation
(spec/def :log/number int?)
(spec/def :log/string string?)
(spec/def :log/type string?)
(spec/def ::log-entry-number (spec/keys :req-un [:log/type, :log/number]))
(spec/def ::log-entry-string (spec/keys :req-un [:log/type, :log/string]))

(defn valid-entry? [log-entry]
  (or
   (spec/valid? ::log-entry-number log-entry)
   (spec/valid? ::log-entry-string log-entry)))

;; Transformation
(defn transform-entry-if-relevant [log-entry]
  (cond (= (:type log-entry) "number")
        (let [number (:number log-entry)]
          (when (> number 900)
            (assoc log-entry :number (Math/log number))))

        (= (:type log-entry) "string")
        (let [string (:string log-entry)]
          (when (re-find #"a" string)
            (update log-entry :string str "-improved!")))))

;; Simulate database insert
(def db (atom 0))

(defn save-into-database [batch]
  (swap! db + (count batch)))

;; Process json files
(defn process-lazy [files]
  (->> files
       (mapcat parse-json-file-lazy) 
       (filter valid-entry?)
       (keep transform-entry-if-relevant)
       (partition-all 1000)          
       (map save-into-database)
       doall))                       

;; Transducer reader that is easily parallelized
;; Relatively low level but can be abstracted away
(defn lines-transducer [^java.io.BufferedReader rdr]
  (reify clojure.lang.IReduceInit
    (reduce [this f init]
      (try
        (loop [state init]
          (if (reduced? state)
            state
            (if-let [line (.readLine rdr)]
              (recur (f state line))
              state)))
        (finally (.close rdr))))))

(defn parse-json-file [file]
  (eduction (map #(json/read-str % :key-fn keyword))
            (lines-transducer (io/reader file))))

;; Now we can parallelize easily
(defn process-parallel [files]
  (a/<!!
   (a/pipeline
    (.availableProcessors (Runtime/getRuntime)) 
    (doto (a/chan) (a/close!))                  
    (comp (mapcat parse-json-file)              
          (filter valid-entry?)
          (keep transform-entry-if-relevant)
          (partition-all 1000)
          (map save-into-database))
    (a/to-chan files))))

(defn -main []
  (println "--- Time how we do for a single file ---")

  (println "\nLazy:")
  (time (process-lazy ["data/dummy.json"]))

  (println "\nParallel:")
  (time (process-parallel ["data/dummy.json"]))

  (println "\n--- What if we run on 10 files ---")

  (println "\nLazy:")
  (time (process-lazy (repeat 10 "data/dummy.json")))

  (println "\nParallel:")
  (time (process-parallel (repeat 10 "data/dummy.json"))))
