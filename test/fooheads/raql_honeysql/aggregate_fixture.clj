(ns fooheads.raql-honeysql.aggregate-fixture
  (:require
    [clojure.string :as str]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql-honeysql.pagination-fixture :as pagination]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(def heading-relmap
  {:relvar [#:relvar{:name :person}
            #:relvar{:name :integers}
            #:relvar{:name :doubles}]
   :attr [#:attr{:name :person/id :type :integer :relvar-name :person}
          #:attr{:name :person/age :type :integer :relvar-name :person}
          #:attr{:name :person/score :type :double :relvar-name :person}
          #:attr{:name :number/id :type :integer :relvar-name :integers}
          #:attr{:name :number/value :type :integer :relvar-name :integers}
          #:attr{:name :float/id :type :integer :relvar-name :doubles}
          #:attr{:name :float/value :type :double :relvar-name :doubles}]})


(def people
  (mapv #(zipmap [:person/id :person/age :person/score] %)
        [[1 20 1.5] [2 20 2.5] [3 50 3.5] [4 21 4.5] [5 nil nil]
         [1 20 1.5]]))


(def numeric-people
  (filterv #(< (:person/id %) 5) people))


(def aggregates
  '{:summary/count [count]
    :summary/sum [sum :person/age]
    :summary/avg [avg :person/age]
    :summary/score-sum [sum :person/score]
    :summary/score-avg [avg :person/score]})


(defn reference
  "Independent set-and-fold expectations, not a production execution backend."
  [rows aggregates]
  (let [rows (set rows)
        types (into {} (map (juxt :attr/name :attr/type)) (:attr heading-relmap))]
    (into {}
          (map (fn [[output [op attr]]]
                 (let [values (map attr rows)]
                   [output
                    (case op
                      count (count rows)
                      sum (reduce + (if (= :double (types attr)) 0.0 0) values)
                      avg (/ (double (reduce + 0 values)) (count rows)))])))
          aggregates)))


(def cases
  [{:name "complete tuples, not selected values, are distinct"
    :source '[restrict [relation :person] [<= :person/id 3]]
    :rows (filter #(< (:person/id %) 4) people) :bindings [3]}
   {:name "singleton"
    :source '[restrict [relation :person] [= :person/id 2]]
    :rows (filter #(= 2 (:person/id %)) people) :bindings [2]}
   {:name "fractional integer average"
    :source '[restrict [relation :person] [<= :person/id 4]]
    :rows numeric-people :bindings [4]}
   {:name "empty count and sum"
    :source '[restrict [relation :person] [< :person/id 0]]
    :rows [] :bindings [0]
    :aggregates '{:summary/count [count] :summary/sum [sum :person/age]
                  :summary/score-sum [sum :person/score]}}
   {:name "project before folding"
    :source '[project [restrict [relation :person] [<= :person/id 4]]
              [:person/age]]
    :rows (map #(select-keys % [:person/age]) numeric-people) :bindings [4]
    :aggregates '{:summary/count [count] :summary/sum [sum :person/age]
                  :summary/avg [avg :person/age]}}
   {:name "page before folding"
    :source '[limit [offset [order-by [distinct [relation :person]] [:person/id]] 2] 2]
    :rows [(nth people 2) (nth people 3)] :bindings [2 2]}
   {:name "presentation ordering does not affect a fold"
    :source '[order-by [restrict [relation :person] [<= :person/id 4]]
              [[:person/id :desc]]]
    :rows numeric-people :bindings [4]}
   {:name "parameters remain in their input scope"
    :source '[limit [offset
                     [order-by
                      [restrict [relation :person]
                       [and [>= :person/id ?minimum] [<= :person/id ?maximum]]]
                      [:person/id]]
                     ?start]
              ?size]
    :params {:minimum 2 :maximum 4 :start 1 :size 2}
    :bindings {:postgresql [2 4 2 1] :mssql [2 4 1 2] :hsqldb [2 4 1 2]}
    :rows [(nth people 2) (nth people 3)]}])


(defn statement
  [expr opts]
  (let [opts (update opts :params #(or % {}))]
    (sql/format (rh/transform (raql/compile heading-relmap expr) opts)
                {:dialect (pagination/dialect (:db-type opts))
                 :params (:params opts)
                 :quoted (:quoted opts)})))


(defn execute!
  [connection expr opts]
  (jdbc/execute! connection (statement expr opts)
                 {:builder-fn rs/as-unqualified-maps}))


(defn with-database
  [db-type jdbc-url f]
  (let [prefix (str "raql_aggregate_" (str/replace (str (random-uuid)) "-" "") "_")
        table-name #(str prefix (name %))
        table #(str "\"" (table-name %) "\"")
        float-type (if (= :mssql db-type) "FLOAT" "DOUBLE PRECISION")
        tables [[:person (str "\"id\" BIGINT, \"age\" BIGINT, \"score\" " float-type)
                 (mapv (juxt :person/id :person/age :person/score) people)]
                [:integers "\"id\" BIGINT, \"value\" BIGINT"
                 [[1 Long/MAX_VALUE] [2 1]]]
                [:doubles (str "\"id\" BIGINT, \"value\" " float-type)
                 [[1 Double/MAX_VALUE] [2 Double/MAX_VALUE]]]]
        created (atom [])]
    (with-open [connection (jdbc/get-connection jdbc-url)]
      (when (= :mssql db-type)
        (jdbc/execute! connection ["SET QUOTED_IDENTIFIER ON"]))
      (try
        (doseq [[name columns rows] tables]
          (jdbc/execute! connection [(str "CREATE TABLE " (table name) " (" columns ")")])
          (swap! created conj name)
          (doseq [row rows]
            (jdbc/execute! connection
                           (into [(str "INSERT INTO " (table name) " VALUES ("
                                       (str/join ", " (repeat (count row) "?")) ")")]
                                 row))))
        (f connection {:db-type db-type :table-name-fn table-name})
        (finally
          (doseq [name (reverse @created)]
            (jdbc/execute! connection [(str "DROP TABLE " (table name))])))))))
