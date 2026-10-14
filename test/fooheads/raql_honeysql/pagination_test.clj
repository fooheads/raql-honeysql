(ns fooheads.raql-honeysql.pagination-test
  (:require
    [clojure.test :refer [deftest is testing]]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql-honeysql.pagination-fixture :as fixture]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]))


(defn honey
  ([expr] (honey expr {}))
  ([expr opts]
   (rh/transform (raql/compile fixture/heading-relmap expr) opts)))


(def base (honey '[relation :item]))
(def wrapper {:select :* :from [[base :__relation]]})
(def ordering [[[:raw "\"item/id\""] :asc]])


(deftest pagination-maps-test
  (doseq [db-type [:postgresql :mssql :hsqldb]]
    (let [opts {:db-type db-type}
          ordered (assoc wrapper :order-by ordering)]
      (testing (str db-type)
        (is (= (case db-type
                 :postgresql (assoc wrapper :limit 3)
                 :mssql (assoc (dissoc wrapper :select) :select-top [3 :*])
                 :hsqldb (assoc wrapper :fetch 3))
               (honey '[limit [relation :item] 3] opts)))
        (is (= (assoc ordered :offset 2)
               (honey (fixture/query '[[order-by [:item/id]] [offset 2]]) opts)))
        (is (= (assoc ordered :offset 2
                      (if (= :postgresql db-type) :limit :fetch) 3)
               (honey (fixture/query '[[order-by [:item/id]] [offset 2] [limit 3]])
                      opts)))
        (is (= (assoc wrapper :where [:= [:inline 1] [:inline 0]])
               (honey (fixture/query '[[order-by [:item/id]] [limit 0]]) opts)))))))


(deftest pagination-sql-test
  (doseq [db-type [:postgresql :mssql :hsqldb]]
    (let [opts {:db-type db-type :params {:start 2 :page-size 3}}
          format-opts {:dialect (fixture/dialect db-type) :params (:params opts)}
          alias (if (= :mssql db-type) "[__relation]" "\"__relation\"")
          source (str "FROM (SELECT \"id\" AS \"item/id\", \"name\" AS \"item/name\""
                      " FROM \"item\" AS " alias ") AS " alias)
          ordered (str "SELECT * " source " ORDER BY \"item/id\" ASC")
          statement (case db-type
                      :postgresql [(str ordered " LIMIT ? OFFSET ?") 3 2]
                      [(str ordered " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY") 2 3])]
      (testing (str db-type)
        (is (= statement
               (sql/format
                 (honey (fixture/query '[[order-by [:item/id]]
                                         [offset ?start] [limit ?page-size]]) opts)
                 format-opts)))
        (is (= (case db-type
                 :postgresql [(str "SELECT * " source " LIMIT ?") 3]
                 :mssql [(str "SELECT TOP(?) * " source) 3]
                 :hsqldb [(str "SELECT * " source " FETCH FIRST ? ROWS ONLY") 3])
               (sql/format (honey '[limit [relation :item] 3] opts) format-opts)))
        (is (= [(str ordered (if (= :mssql db-type) " OFFSET ? ROWS" " OFFSET ?")) 2]
               (sql/format
                 (honey (fixture/query '[[order-by [:item/id]] [offset 2]]) opts)
                 format-opts)))
        (is (= [(str "SELECT * " source " WHERE 1 = 0")]
               (sql/format
                 (honey (fixture/query '[[order-by [:item/id]] [limit ?page-size]])
                        (assoc opts :params {:page-size 0}))
                 format-opts)))))))


(deftest pagination-window-test
  (doseq [db-type [:postgresql :mssql :hsqldb]]
    (let [opts {:db-type db-type}
          combined (honey (fixture/query '[[order-by [:item/id]] [limit 3 2]]) opts)]
      (is (= combined
             (honey (fixture/query '[[order-by [:item/id]] [offset 2] [limit 3]]) opts)))
      (is (= combined
             (honey (fixture/query '[[order-by [:item/id]] [limit 5] [offset 2]]) opts)))
      (is (= combined
             (honey (fixture/query '[[order-by [:item/id]] [offset 1] [offset 1]
                                     [limit 5] [limit 3]]) opts)))
      (is (= (honey (fixture/query '[[order-by [:item/id]] [limit 0]]) opts)
             (honey (fixture/query '[[order-by [:item/id]] [limit 2] [offset 5]]) opts))))))


(deftest invalid-pagination-test
  (doseq [params [{} {:page-size nil} {:page-size -1} {:page-size 1.5}
                  {:page-size "3"} {:page-size true}]]
    (is (thrown? clojure.lang.ExceptionInfo
          (honey '[limit [relation :item] ?page-size] {:params params}))))
  (doseq [opts [{:db-type :unknown} {:db-type nil} {:params []}]]
    (is (thrown? clojure.lang.ExceptionInfo (honey '[relation :item] opts))))
  (doseq [expr '[[offset [relation :item] 2]
                 [offset [relation :item] 0]
                 [limit [relation :item] 3 0]
                 [limit [relation :item] 0 2]
                 [project [order-by [relation :item] [:item/id]] [:item/id]]]]
    (is (thrown? clojure.lang.ExceptionInfo (honey expr {:db-type :mssql}))))
  (is (= (honey '[limit [relation :item] 3] {:db-type :postgresql})
         (honey '[limit [relation :item] 3]))))


(deftest hsqldb-pagination-test
  (fixture/with-database
    :hsqldb (str "jdbc:hsqldb:mem:raql_" (random-uuid))
    fixture/assert-pages!))
