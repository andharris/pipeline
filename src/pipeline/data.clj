(ns pipeline.data
  (:require [clojure.java.io :as io]
            [clojure.data.json :as json]))

(defn create-dummy-data! []
  (letfn [(rand-obj []
            (case (rand-int 3)
              0 {:type "number" :number (rand-int 1000)}
              1 {:type "string" :string (apply str (repeatedly 30 #(char (+ 33 (rand-int 90)))))}
              2 {:type "empty"}))]
    (with-open [f (io/writer "data/dummy.json")]
      (binding [*out* f]
        (dotimes [_ 100000]
          (println (json/write-str (rand-obj))))))))
