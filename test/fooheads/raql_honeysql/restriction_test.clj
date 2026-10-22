(ns fooheads.raql-honeysql.restriction-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest is testing]]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql-honeysql.divide-fixture :as fixture]
    [fooheads.raql-honeysql.pagination-fixture :as pagination]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(def projection
  ['project ['restrict fixture/dividend '[<= :item/id ?maximum]] [:item/id]])


(def paged-projection
  ['-> fixture/dividend
   ['project [:item/id]] ['distinct] ['restrict '[>= :item/id ?sub-minimum]]
   ['order-by [:item/id]] ['offset '?start] ['limit '?size]])


(def nested-division
  ['divide
   ['divide fixture/dividend ['project fixture/divisor [:tag/value]]]
   ['project fixture/divisor [:tag/locale]]])


(defn query
  [predicate]
  ['-> fixture/dividend ['restrict predicate]
   ['project [:item/id]] ['distinct] ['order-by [:item/id]]])


(defn honey
  [expr opts]
  (rh/transform (raql/compile fixture/heading-relmap expr) opts))


(def cases
  [{:name "standalone projection"
    :predicate ['in :item/id projection]
    :params {:maximum 3} :bindings [3] :expected [1 2 3]}
   {:name "projection inside AND"
    :predicate ['and '[> :item/id ?floor] ['in :item/id projection]]
    :params {:floor 1 :maximum 3} :bindings [1 3] :expected [2 3]}
   {:name "projection inside OR"
    :predicate ['or '[= :item/id ?extra] ['in :item/id projection]]
    :params {:extra 8 :maximum 3} :bindings [8 3] :expected [1 2 3 8]}
   {:name "projection inside nested AND and OR"
    :predicate ['and
                ['or '[= :item/id ?extra] ['in :item/id projection]]
                '[< :item/id ?ceiling]]
    :params {:extra 8 :maximum 3 :ceiling 4}
    :bindings [8 3 4] :expected [1 2 3]}
   {:name "standalone division"
    :predicate ['in :item/id fixture/parameterized]
    :params {:minimum 2 :tag "A"} :bindings [2 "A" 2] :expected [2 3 5 8]}
   {:name "division inside AND"
    :predicate ['and '[> :item/id ?floor] ['in :item/id fixture/parameterized]]
    :params {:floor 2 :minimum 2 :tag "A"}
    :bindings [2 2 "A" 2] :expected [3 5 8]}
   {:name "division inside OR"
    :predicate ['or '[= :item/id ?extra] ['in :item/id fixture/parameterized]]
    :params {:extra 1 :minimum 2 :tag "A"}
    :bindings [1 2 "A" 2] :expected [1 2 3 5 8]}
   {:name "division and projection inside nested OR and AND"
    :predicate ['or
                ['and '[> :item/id ?floor] ['in :item/id fixture/parameterized]]
                ['and '[= :tag/value ?outer-tag] ['in :item/id projection]]]
    :params {:floor 3 :minimum 2 :tag "A" :outer-tag "B" :maximum 3}
    :bindings [3 2 "A" 2 "B" 3] :expected [1 3 5 8]}
   {:name "literal list and collection parameter alongside a subquery"
    :predicate ['and '[in :item/id ?ids]
                ['or '[in :item/id [1 8]] ['in :item/id projection]]]
    :params {:ids [2 3 8] :maximum 3}
    :bindings [2 3 8 1 8 3] :expected [2 3 8]}
   {:name "literal strings alongside a subquery"
    :predicate ['and '[in :tag/value ["A" "B"]] ['in :item/id projection]]
    :params {:maximum 3} :bindings ["A" "B" 3] :expected [1 2 3]}
   {:name "NOT around a subquery"
    :predicate ['not ['in :item/id projection]]
    :params {:maximum 3} :bindings [3] :expected [4 5 6 7 8]}
   {:name "paged subquery preserves options and binding order"
    :predicate ['and '[> :item/id ?floor] ['in :item/id paged-projection]
                '[< :item/id ?ceiling]]
    :params {:floor 0 :sub-minimum 1 :start 1 :size 2 :ceiling 8}
    :bindings {:postgresql [0 1 2 1 8] :mssql [0 1 1 2 8] :hsqldb [0 1 1 2 8]}
    :expected [2 3]}
   {:name "nested divisions and multiple subqueries"
    :predicate ['and ['in :item/id nested-division]
                ['or ['in :item/id fixture/parameterized] ['in :item/id projection]]]
    :params {:minimum 2 :tag "A" :maximum 3}
    :bindings [2 "A" 2 3] :expected [1 3 8]}
   {:name "Boolean join condition with an IN subquery"
    :expr ['->
           ['join fixture/dividend
            ['rename ['project fixture/dividend [:item/id]] [[:item/id :other/id]]]
            ['and '[= :item/id :other/id] ['in :item/id projection]]]
           ['project [:item/id]] ['distinct] ['order-by [:item/id]]]
    :params {:maximum 3} :bindings [3] :expected [1 2 3]}])


(deftest nested-in-translation-test
  (doseq [subquery [projection fixture/parameterized]]
    (let [where (:where (honey ['restrict fixture/dividend
                                ['and ['in :item/id subquery] '[> :item/id 0]]]
                               {}))
          translated (get-in where [1 2])]
      (is (= 'and (first where)))
      (is (not (contains? translated :operator)))
      (is (or (:select translated) (:select-distinct translated))))))


(deftest nested-in-options-and-aliases-test
  (let [opts {:db-schema :custom
              :table-name-fn #(str "mapped_" (name %))
              :column-name-fn (fn [_ attr] (str "column_" (name attr)))}
        predicate ['in :item/id projection]
        standalone (:where (honey ['restrict fixture/dividend predicate] opts))
        nested (:where (honey ['restrict fixture/dividend ['and predicate '[> :item/id 0]]]
                              opts))]
    (is (= standalone (second nested)))
    (is (str/includes?
          (first (sql/format {:select :* :from [[(nth standalone 2) :subquery]]}
                             {:params {:maximum 3}}))
          "\"custom\".\"mapped_item-tags\""))
    (is (str/includes?
          (first (sql/format {:select :* :from [[(nth standalone 2) :subquery]]}
                             {:params {:maximum 3}}))
          "\"column_id\"")))
  (let [predicate ['in :item/id fixture/division]
        where (:where (honey ['restrict fixture/dividend ['and predicate predicate]] {}))
        aliases (mapv #(get-in % [2 :from 0 1]) (rest where))]
    (is (= 2 (count (distinct aliases)))))
  (doseq [literal '[[in :item/id [1 3]]
                    [in :item/id ?ids]
                    [in :tag/value ["and" "or"]]]]
    (let [standalone (:where (honey ['restrict fixture/dividend literal] {}))
          nested (:where (honey ['restrict fixture/dividend ['or literal '[> :item/id 0]]]
                                {}))]
      (is (= standalone (second nested))))))


(deftest nested-in-dialects-test
  (doseq [db-type [:postgresql :mssql :hsqldb]
          {:keys [name expr predicate params bindings]} cases]
    (testing (str db-type " " name)
      (let [query (honey (or expr (query predicate)) {:db-type db-type :params params})
            [statement & values] (sql/format query
                                             {:dialect (pagination/dialect db-type)
                                              :params params})]
        (is (= (if (map? bindings) (bindings db-type) bindings) values))
        (is (str/includes? statement " IN "))
        (is (str/includes? statement "\"item/id\""))))))


(defn assert-restrictions!
  [connection opts]
  (doseq [{:keys [name expr predicate params expected]} cases]
    (testing (str (:db-type opts) " " name " " (:format-opts opts))
      (let [query (honey (or expr (query predicate)) (assoc opts :params params))
            statement (sql/format query (merge
                                          {:dialect (pagination/dialect (:db-type opts))
                                           :params params}
                                          (:format-opts opts)))
            rows (jdbc/execute! connection statement
                                {:builder-fn rs/as-unqualified-maps})]
        (is (= expected (mapv :item/id rows)))))))


(deftest hsqldb-nested-in-test
  (fixture/with-database
    :hsqldb (str "jdbc:hsqldb:mem:raql_restriction_" (random-uuid))
    (fn [connection opts]
      (assert-restrictions! connection opts)
      (assert-restrictions! connection (assoc opts :format-opts {:quoted false})))))


(deftest sqlite-nested-in-test
  (fixture/with-database :postgresql "jdbc:sqlite::memory:" assert-restrictions!))
