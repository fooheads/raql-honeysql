(ns fooheads.raql-honeysql.aggregate-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [fooheads.raql-honeysql.aggregate-fixture :as fixture]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql.core :as raql]
    [fooheads.raql.rewrite :as rewrite]
    [honey.sql :as sql]))


(defn assert-summary!
  [connection opts expr expected]
  (let [rows (fixture/execute! connection expr opts)
        actual (first rows)]
    (is (= 1 (count rows)))
    (is (= (set (keys expected)) (set (keys actual))))
    (doseq [[attr value] expected]
      (if (integer? value)
        (do (is (integer? (actual attr)))
            (is (= value (actual attr))))
        (do (is (double? (actual attr)))
            (is (< (Math/abs (- value (actual attr))) 1.0E-9)))))))


(defn assert-numeric-cases!
  [connection opts]
  (doseq [{:keys [name source rows aggregates params]} fixture/cases
          :let [aggregates (or aggregates fixture/aggregates)
                expr ['aggregate source aggregates]]]
    (testing name
      (assert-summary! connection (assoc opts :params params) expr
                       (fixture/reference rows aggregates))))
  (let [views '{:matching [restrict [relation :person] [<= :person/id ?maximum]]}
        matching '[project [relation :matching] [:person/id]]
        count-expr (rewrite/apply-views
                     views ['aggregate matching '{:summary/count [count]}])
        page-expr (rewrite/apply-views
                    views ['-> matching ['distinct] ['order-by [:person/id]]
                           ['offset 1] ['limit 2]])
        opts (assoc opts :params {:maximum 4})]
    (assert-summary! connection opts count-expr {:summary/count 4})
    (is (= [2 3] (mapv :person/id (fixture/execute! connection page-expr opts)))))
  (let [summary '[aggregate [restrict [relation :person] [<= :person/id 4]]
                  {:n [count] :mean [avg :person/age]}]]
    (assert-summary! connection opts
                     ['-> summary ['project [:mean]] ['restrict '[> :mean 20]]
                      ['order-by [:mean]] ['offset 0] ['limit 1]]
                     {:mean 27.75})
    (is (= [] (fixture/execute! connection ['offset ['order-by summary [:n]] 1] opts)))
    (assert-summary! connection opts
                     ['aggregate summary '{:n [count] :total [sum :n]}]
                     {:n 1 :total 4})
    (is (= [{:n 4 :mean 27.75 :extra "x"}]
           (fixture/execute! connection ['extend summary {:extra [:string "x"]}] opts))))
  (assert-summary! connection opts
                   '[aggregate [relation :person] {:operator [count] :args [count]}]
                   {:operator 5 :args 5})
  (assert-summary! connection opts
                   '[aggregate [relation :integers] {:mean [avg :number/value]}]
                   {:mean (/ (double Long/MAX_VALUE) 2)}))


(defn assert-count-cases!
  [connection opts]
  (doseq [[source expected]
          [['[relation :person] 5]
           ['[project [relation :person] [:person/age]] 4]
           ['[restrict [relation :person] [< :person/id 0]] 0]
           ['[project [relation :person] []] 1]
           ['[project [restrict [relation :person] [< :person/id 0]] []] 0]
           ['[divide [project [relation :person] [:person/id :person/age]]
              [project [restrict [relation :person] [<= :person/id 2]] [:person/age]]] 2]
           ['[aggregate [relation :person] {}] 1]
           ['[aggregate [restrict [relation :person] [< :person/id 0]] {}] 1]]]
    (assert-summary! connection opts ['aggregate source '{:n [count]}] {:n expected}))
  (assert-summary! connection opts
                   '[project
                     [restrict [relation :person]
                      [and [in :person/id
                            [aggregate [restrict [relation :person] [<= :person/id 3]]
                             {:n [count]}]]
                       [> :person/id 0]]]
                     [:person/id]]
                   {:person/id 3})
  (doseq [source ['[relation :person]
                  '[restrict [relation :person] [< :person/id 0]]]]
    (is (= [{:__raql_unit 1}]
           (fixture/execute! connection ['aggregate source {}] opts)))))


(defn assert-errors!
  [connection opts]
  (doseq [[expr overflow?]
          [['[aggregate [restrict [relation :person] [< :person/id 0]]
              {:mean [avg :person/age]}] false]
           ['[aggregate [relation :person] {:total [sum :person/age]}] false]
           ['[aggregate [relation :person] {:mean [avg :person/age]}] false]
           ['[aggregate [relation :person] {:total [sum :person/score]}] false]
           ['[aggregate [relation :person] {:mean [avg :person/score]}] false]
           ['[project
              [aggregate [restrict [relation :person] [< :person/id 0]]
               {:n [count] :mean [avg :person/age]}] [:n]] false]
           ['[aggregate
              [project
               [aggregate [restrict [relation :person] [< :person/id 0]]
                {:n [count] :mean [avg :person/age]}] [:n]]
              {:n [count]}] false]
           ['[aggregate [relation :integers] {:total [sum :number/value]}] true]
           ['[aggregate [relation :doubles] {:total [sum :float/value]}] true]]]
    (testing (str expr)
      (try
        (fixture/execute! connection expr opts)
        (is false "Expected a cardinality or numeric overflow error")
        (catch java.sql.SQLException e
          (is (or (= "21000" (.getSQLState e))
                  (and overflow? (= "22003" (.getSQLState e)))
                  (and (= :mssql (:db-type opts))
                       (contains? (if overflow? #{512 8115} #{512}) (.getErrorCode e))))
              (str (.getSQLState e) " " (.getMessage e))))))))


(deftest embedded-aggregate-execution-test
  (fixture/with-database
    :hsqldb "jdbc:hsqldb:mem:raql_aggregate_tests"
    (fn [connection opts]
      (doseq [quoted [false true]
              :let [opts (assoc opts :quoted quoted)]]
        (assert-numeric-cases! connection opts)
        (assert-count-cases! connection opts)
        (assert-errors! connection opts))))
  (fixture/with-database
    :sqlite "jdbc:sqlite::memory:"
    (fn [connection opts]
      (assert-count-cases! connection opts))))


(deftest aggregate-dialect-generation-test
  (doseq [db-type [:postgresql :mssql :hsqldb]
          quoted [false true]
          {:keys [source params aggregates bindings]} fixture/cases
          :let [expr ['aggregate source (or aggregates fixture/aggregates)]
                expected-bindings (if (map? bindings) (bindings db-type) bindings)
                [statement & actual-bindings] (fixture/statement
                                                expr {:db-type db-type :params params
                                                      :quoted quoted})]]
    (is (str/includes? statement "SELECT DISTINCT"))
    (is (= (boolean (some #(= 'avg (first %))
                          (vals (or aggregates fixture/aggregates))))
           (str/includes? statement "AVG(CAST(")))
    (is (str/includes? statement (if (= :mssql db-type) "COUNT_BIG(*)" "COUNT(*)")))
    (is (str/includes? statement (case db-type
                                   :postgresql "DOUBLE PRECISION"
                                   :mssql "FLOAT"
                                   :hsqldb "DOUBLE")))
    (is (str/includes? statement "VALUES (0), (1)"))
    (is (= expected-bindings actual-bindings)))
  (let [expr '[aggregate [relation :person] {:n [count]}]
        ast (raql/compile fixture/heading-relmap expr)]
    (is (= (rh/transform ast) (rh/transform ast {:db-type :postgresql})))
    (is (not (str/includes? (first (fixture/statement expr {})) "VALUES")))
    (let [statement (first (fixture/statement expr {:db-type :mssql}))]
      (is (str/includes? statement "COUNT_BIG(*)"))
      (is (str/includes? statement "VALUES (0), (1)"))
      (is (str/includes? statement "[raql_aggregate_count] IS NULL"))))
  (let [expr '[aggregate [relation :person] {:n [count] :mean [avg :person/age]}]
        opts {:db-type :mssql :db-schema :custom
              :table-name-fn #(str "mapped_" (name %))
              :column-name-fn (fn [_ attr] (str "column_" (name attr)))}
        honey (rh/transform (raql/compile fixture/heading-relmap expr) opts)
        formatted (first (sql/format honey {:dialect :sqlserver}))]
    (is (str/includes? formatted "\"custom\".\"mapped_person\""))
    (is (str/includes? formatted "\"column_age\""))
    (is (not (re-find #"(?<!_)COUNT\(" formatted)))))


(deftest aggregate-parameter-scope-test
  (doseq [db-type [:postgresql :mssql :hsqldb]
          :let [expr '[-> [relation :person]
                       [restrict [<= :person/id ?maximum]]
                       [aggregate {:n [count] :mean [avg :person/age]}]
                       [restrict [and [>= :n ?minimum-count] [< :mean ?ceiling]]]]
                opts {:db-type db-type
                      :params {:maximum 4 :minimum-count 2 :ceiling 30}}
                [statement & bindings] (fixture/statement expr opts)]]
    (is (str/includes? statement "\"n\" >= ?"))
    (is (= [4 2 30] bindings))))


(deftest sqlite-strict-aggregate-capability-test
  (doseq [op '[sum avg]]
    (try
      (fixture/statement ['aggregate '[relation :person] {:n [op :person/age]}]
                         {:db-type :sqlite})
      (is false "Expected explicit unsupported-capability error")
      (catch clojure.lang.ExceptionInfo e
        (is (= :sqlite (:db-type (ex-data e))))
        (is (= :scalar-subquery-cardinality (:capability (ex-data e)))))))
  (is (str/includes?
        (first (fixture/statement
                 '[limit [order-by [aggregate [relation :person] {:n [count]}] [:n]] 1]
                 {:db-type :sqlite}))
        "LIMIT ?")))
