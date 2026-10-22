(ns fooheads.raql-honeysql.divide-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql-honeysql.divide-fixture :as fixture]
    [fooheads.raql-honeysql.pagination-fixture :as pagination]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]))


(defn honey
  [expr opts]
  (rh/transform (raql/compile fixture/heading-relmap expr) opts))


(deftest division-structures-test
  (let [query (honey fixture/division {})
        missing (get-in query [:where 1 1])
        witness (get-in missing [:where 1 1])]
    (is (= [:not :exists] [(first (:where query)) (first (second (:where query)))]))
    (is (= [:not :exists] [(first (:where missing)) (first (second (:where missing)))]))
    (is (= [[[:. :raql_divide_candidate_1 "__divide_attr_0"]]
            [[:raw "\"item/id\""]]]
           (first (:select-distinct query))))
    (is (= [:and
            [:= [:. :raql_divide_witness_1 "__divide_attr_0"]
             [:. :raql_divide_candidate_1 "__divide_attr_0"]]
            [:= [:. :raql_divide_witness_1 "__divide_attr_1"]
             [:. :raql_divide_required_1 "__divide_attr_1"]]
            [:= [:. :raql_divide_witness_1 "__divide_attr_2"]
             [:. :raql_divide_required_1 "__divide_attr_2"]]]
           (:where witness)))
    (doseq [db-type [:postgresql :mssql :hsqldb]]
      (is (= query (honey fixture/division {:db-type db-type}))))
    (is (= query (honey fixture/division {})))))


(deftest division-dialects-test
  (doseq [db-type [:postgresql :mssql :hsqldb]]
    (testing (str db-type)
      (let [expr ['-> fixture/parameterized ['order-by [:item/id]]
                  ['offset '?start] ['limit '?size]]
            opts {:db-type db-type :params {:minimum 2 :tag "A" :start 1 :size 2}}
            query (honey expr opts)
            [statement & parameters] (sql/format
                                       query
                                       {:dialect (pagination/dialect db-type)
                                        :params (:params opts)})]
        (is (str/includes? statement "SELECT DISTINCT"))
        (is (= 2 (count (re-seq #"NOT EXISTS" statement))))
        (is (= (if (= :postgresql db-type) [2 "A" 2 2 1] [2 "A" 2 1 2])
               parameters))
        (is (not (str/includes? statement "'A'")))
        (is (str/ends-with? statement
                            (case db-type
                              :postgresql "LIMIT ? OFFSET ?"
                              "OFFSET ? ROW FETCH NEXT ? ROWS ONLY")))
        (is (str/includes? statement
                           (if (= :mssql db-type)
                             "[raql_divide_witness_1].[__divide_attr_1]"
                             "\"raql_divide_witness_1\".\"__divide_attr_1\""))))
      (doseq [{:keys [expr params name]} fixture/cases]
        (testing name
          (is (vector? (sql/format
                         (honey expr {:db-type db-type :params (or params {})})
                         {:dialect (pagination/dialect db-type)
                          :params (or params {})}))))))))


(deftest division-parameter-errors-test
  (doseq [params [{} {:minimum 2} {:tag "A"}]]
    (is (thrown? clojure.lang.ExceptionInfo
          (sql/format (honey fixture/parameterized {}) {:params params})))))


(deftest division-sqlserver-ordering-test
  (doseq [expr [['divide ['order-by fixture/dividend [:item/id]] fixture/divisor]
                ['divide fixture/dividend ['order-by fixture/divisor [:tag/value]]]]]
    (is (thrown? clojure.lang.ExceptionInfo (honey expr {:db-type :mssql})))))


(deftest hsqldb-division-test
  (fixture/with-database
    :hsqldb (str "jdbc:hsqldb:mem:raql_divide_" (random-uuid))
    (fn [connection opts]
      (fixture/assert-division! connection opts)
      (fixture/assert-division! connection (assoc opts :format-opts {:quoted false})))))


(deftest sqlite-division-test
  (fixture/with-database :postgresql "jdbc:sqlite::memory:" fixture/assert-division!))
