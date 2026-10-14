(ns fooheads.raql-honeysql.pagination-fixture
  (:require
    [clojure.string :as str]
    [clojure.test :refer [is testing]]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(def heading-relmap
  {:relvar [#:relvar{:name :item} #:relvar{:name :other}]
   :attr [#:attr{:name :item/id :type :integer :relvar-name :item}
          #:attr{:name :item/name :type :string :relvar-name :item}
          #:attr{:name :other/item-id :type :integer :relvar-name :other}]})


(defn query
  [steps]
  (into '[-> [relation :item]] steps))


(def cases
  [{:steps '[[order-by [:item/id]] [limit 3]] :expected [1 2 3]}
   {:steps '[[order-by [:item/id]] [offset 2]] :expected [3 4 5 6 7 8]}
   {:steps '[[order-by [:item/id]] [offset 0]] :expected [1 2 3 4 5 6 7 8]}
   {:steps '[[order-by [:item/id]] [offset 2] [limit 3]] :expected [3 4 5]}
   {:steps '[[order-by [:item/id]] [limit 3 2]] :expected [3 4 5]}
   {:steps '[[order-by [:item/id]] [limit 0]] :expected []}
   {:steps '[[order-by [:item/id]] [offset 2] [limit 0]] :expected []}
   {:steps '[[order-by [:item/id]] [offset 20]] :expected []}
   {:steps '[[order-by [:item/id]] [offset 2] [offset 1] [limit 2]] :expected [4 5]}
   {:steps '[[order-by [:item/id]] [limit 5] [limit 2]] :expected [1 2]}
   {:steps '[[order-by [:item/id]] [limit 5] [offset 2]] :expected [3 4 5]}
   {:steps '[[order-by [:item/id]] [limit 2] [offset 5]] :expected []}
   {:steps '[[order-by [:item/id]] [limit 3] [order-by [[:item/id :desc]]]]
    :expected [3 2 1]}
   {:steps '[[order-by [:item/id]] [limit 5] [order-by [[:item/id :desc]]]
             [offset 1] [limit 2]]
    :expected [4 3]}
   {:steps '[[order-by [:item/name :item/id]] [offset 2] [limit 3]]
    :expected [7 1 5]}
   {:steps '[[restrict [>= :item/id 3]] [order-by [:item/id]] [offset 1] [limit 2]]
    :expected [4 5]}
   {:steps '[[order-by [:item/id]] [limit 4] [restrict [>= :item/id 3]]
             [order-by [:item/id]]]
    :expected [3 4]}
   {:steps '[[order-by [:item/id]] [limit 4] [restrict [>= :item/id 3]]
             [order-by [:item/id]] [offset 1] [limit 2]]
    :expected [4]}
   {:steps '[[project [:item/id]] [order-by [:item/id]] [offset 2] [limit 2]]
    :expected [3 4]}
   {:steps '[[order-by [:item/id]] [offset 2] [limit 2] [project [:item/id]]
             [order-by [:item/id]]]
    :expected [3 4]}
   {:steps '[[order-by [:item/id]] [limit 4] [project-away [:item/name]]
             [order-by [:item/id]] [offset 1] [limit 2]]
    :expected [2 3]}
   {:steps '[[rename [[:item/id :item/key]]] [order-by [:item/key]] [offset 2] [limit 2]]
    :result-key :item/key :expected [3 4]}
   {:steps '[[order-by [:item/id]] [limit 4] [rename [[:item/id :item/key]]]
             [order-by [:item/key]] [offset 1] [limit 2]]
    :result-key :item/key :expected [2 3]}
   {:steps '[[project [:item/name]] [distinct] [order-by [:item/name]]
             [offset 1] [limit 2]]
    :result-key :item/name :expected ["b" "c"]}
   {:steps '[[order-by [:item/id]] [limit 3] [project [:item/name]] [distinct]
             [order-by [:item/name]] [offset 1] [limit 2]]
    :result-key :item/name :expected ["b"]}
   {:steps '[[join [relation :other] [= :item/id :other/item-id]]
             [order-by [:item/id]] [offset 1] [limit 2]]
    :expected [5]}
   {:expr '[-> [union
                [restrict [relation :item] [<= :item/id 4]]
                [restrict [relation :item] [>= :item/id 4]]]
            [order-by [:item/id]] [offset 2] [limit 3]]
    :expected [3 4 5]}
   {:expr '[-> [union
                [limit [order-by [relation :item] [:item/id]] 2]
                [limit [offset [order-by [relation :item] [:item/id]] 6] 2]]
            [order-by [:item/id]]]
    :expected [1 2 7 8]}
   {:steps '[[restrict
              [in :item/id
               [project
                [limit [offset [order-by [relation :item] [:item/id]] 2] 2]
                [:item/id]]]]
             [order-by [:item/id]]]
    :expected [3 4]}
   {:steps '[[extend {:item/rating [:string "x"]}] [order-by [:item/id]] [limit 2]]
    :result-key :item/rating :expected ["x" "x"]}
   {:steps '[[order-by [:item/id]] [limit 2] [extend {:item/rating [:string "x"]}]
             [order-by [:item/id]]]
    :result-key :item/rating :expected ["x" "x"]}
   {:steps '[[order-by [:item/id]] [limit 5] [order-by [:item/id]]
             [extend {:item/rating [:string "x"]}] [offset 1] [limit 2]]
    :expected [2 3]}
   {:steps '[[restrict [>= :item/id ?minimum]] [order-by [:item/id]]
             [offset ?start] [limit ?page-size]]
    :params {:minimum 2 :start 2 :page-size 3} :expected [4 5 6]}
   {:steps '[[order-by [:item/id]] [limit ?page-size ?start]]
    :params {:start 2 :page-size 3} :expected [3 4 5]}
   {:steps '[[order-by [:item/id]] [offset ?start]]
    :params {:start 2} :expected [3 4 5 6 7 8]}
   {:steps '[[order-by [:item/id]] [limit ?page-size]]
    :params {:page-size 0} :expected []}
   {:steps '[[order-by [:item/id]] [offset ?start] [limit ?page-size]]
    :params {:start 0 :page-size 0} :expected []}
   {:steps '[[order-by [:item/id]] [limit ?page-size] [offset ?start]]
    :params {:start 5 :page-size 2} :expected []}
   {:steps '[[order-by [:item/id]] [limit ?paging/size]]
    :params {:paging/size 2} :expected [1 2]}])


(defn dialect
  [db-type]
  (if (= :mssql db-type) :sqlserver :ansi))


(def composition-cases
  (for [a '[limit offset]
        b '[limit offset]
        c '[limit offset]
        n [0 2 5]
        m [0 2 5]
        k [0 2 5]
        :let [steps [[a n] [b m] [c k]]]]
    {:steps (into '[[order-by [:item/id]]] steps)
     :expected (vec (reduce (fn [rows [operator count]]
                              ((case operator limit take offset drop) count rows))
                            (range 1 9)
                            steps))}))


(defn assert-pages!
  [connection opts]
  (doseq [{:keys [steps expr params result-key expected]} (concat cases composition-cases)]
    (let [expr (or expr (query steps))
          params (or params {})]
      (testing (str (:db-type opts) " " expr " " params)
        (let [honey (rh/transform (raql/compile heading-relmap expr)
                                  (assoc opts :params params))
              statement (sql/format honey {:dialect (dialect (:db-type opts))
                                           :params params})
              rows (jdbc/execute! connection statement
                                  {:builder-fn rs/as-unqualified-maps})]
          (is (= expected (mapv (or result-key :item/id) rows)))))))
  (let [honey (rh/transform
                (raql/compile heading-relmap '[limit [relation :item] ?page-size])
                (assoc opts :params {:page-size 0}))
        statement (sql/format honey {:dialect (dialect (:db-type opts))})]
    (with-open [prepared (jdbc/prepare connection statement)]
      (let [metadata (.getMetaData prepared)]
        (is (= ["item/id" "item/name"]
               (mapv #(.getColumnLabel metadata %)
                     (range 1 (inc (.getColumnCount metadata))))))))))


(defn with-database
  [db-type jdbc-url f]
  (let [prefix (str "raql_page_" (str/replace (str (random-uuid)) "-" "") "_")
        table-name #(str prefix (name %))
        item (str "\"" (table-name :item) "\"")
        other (str "\"" (table-name :other) "\"")
        created (atom [])]
    (with-open [connection (jdbc/get-connection jdbc-url)]
      (when (= :mssql db-type)
        (jdbc/execute! connection ["SET QUOTED_IDENTIFIER ON"]))
      (try
        (jdbc/execute! connection
                       [(str "CREATE TABLE " item " (\"id\" INTEGER, \"name\" VARCHAR(40))")])
        (swap! created conj item)
        (jdbc/execute! connection [(str "CREATE TABLE " other " (\"item-id\" INTEGER)")])
        (swap! created conj other)
        (doseq [[id name] [[1 "b"] [2 "a"] [3 "a"] [4 "c"]
                           [5 "b"] [6 "d"] [7 "a"] [8 "c"]]]
          (jdbc/execute! connection
                         [(str "INSERT INTO " item " VALUES (?, ?)") id name]))
        (doseq [id [3 5]]
          (jdbc/execute! connection [(str "INSERT INTO " other " VALUES (?)") id]))
        (f connection {:db-type db-type :table-name-fn table-name})
        (finally
          (doseq [table (reverse @created)]
            (jdbc/execute! connection [(str "DROP TABLE " table)])))))))
