(ns fooheads.raql-honeysql.divide-fixture
  (:require
    [clojure.string :as str]
    [clojure.test :refer [is testing]]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql-honeysql.pagination-fixture :as pagination]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(def heading-relmap
  {:relvar [#:relvar{:name :item-tags} #:relvar{:name :required-tags}]
   :attr [#:attr{:name :item/id :type :integer :relvar-name :item-tags}
          #:attr{:name :tag/value :type :string :relvar-name :item-tags}
          #:attr{:name :tag/locale :type :string :relvar-name :item-tags}
          #:attr{:name :tag/value :type :string :relvar-name :required-tags}
          #:attr{:name :tag/locale :type :string :relvar-name :required-tags}]})


(def dividend '[relation :item-tags])
(def divisor '[relation :required-tags])
(def division ['divide dividend divisor])
(def empty-dividend ['limit dividend 0])
(def empty-divisor ['limit divisor 0])
(def example-dividend ['restrict dividend '[<= :item/id 3]])


(def parameterized
  ['divide
   ['restrict dividend '[>= :item/id ?minimum]]
   ['restrict divisor '[= :tag/value ?tag]]])


(def all-ids [[nil] [1] [2] [3] [4] [5] [6] [7] [8]])


(def cases
  [{:name "tag example, including extra and missing tags"
    :expr ['divide
           ['project example-dividend [:item/id :tag/value]]
           ['project divisor [:tag/value]]]
    :expected [[1] [3]]}
   {:name "multi-attribute divisor, duplicates in both operands"
    :expr division :expected [[1] [3] [8]]}
   {:name "empty divisor retains only distinct candidates from R"
    :expr ['divide dividend empty-divisor] :expected all-ids}
   {:name "empty dividend" :expr ['divide empty-dividend divisor] :expected []}
   {:name "both empty"
    :expr ['divide empty-dividend empty-divisor] :expected []}
   {:name "empty derived restriction"
    :expr ['divide ['restrict dividend '[> :item/id 100]] divisor] :expected []}
   {:name "empty derived divisor"
    :expr ['divide dividend ['restrict divisor '[= :tag/value "absent"]]]
    :expected all-ids}
   {:name "multi-attribute result and NULL candidate components"
    :expr ['divide dividend ['project divisor [:tag/value]]]
    :expected [[1 "en"] [3 "en"] [4 "fr"] [8 "en"] [8 "fr"]]}
   {:name "restriction parameters in both operands"
    :expr parameterized :params {:minimum 2 :tag "A"}
    :expected [[2] [3] [5] [8]]}
   {:name "derived operands retain their pagination scopes"
    :expr ['divide
           ['limit ['order-by example-dividend [:item/id :tag/value]] 3]
           ['limit ['order-by divisor [:tag/value]] 3]]
    :expected [[1]]}
   {:name "divisor pagination changes required tuples"
    :expr ['divide example-dividend
           ['limit ['order-by divisor [:tag/value]] 1]]
    :expected [[1] [2] [3]]}
   {:name "nested division in dividend"
    :expr ['divide
           ['divide dividend ['project divisor [:tag/value]]]
           ['project divisor [:tag/locale]]]
    :expected [[1] [3] [8]]}
   {:name "nested division in divisor"
    :expr ['divide dividend
           ['divide divisor ['project divisor [:tag/locale]]]]
    :expected [[1 "en"] [3 "en"] [4 "fr"] [8 "en"] [8 "fr"]]}
   {:name "division inside IN subquery"
    :expr ['distinct
           ['project ['restrict dividend ['in :item/id division]] [:item/id]]]
    :expected [[1] [3] [8]]}
   {:name "projection and restriction after division"
    :expr ['project ['restrict division '[> :item/id 1]] [:item/id]]
    :expected [[3] [8]]}
   {:name "outer ordering, offset and limit"
    :expr ['-> dividend ['divide divisor] ['order-by [:item/id]]
           ['offset '?start] ['limit '?size]]
    :params {:start 1 :size 1} :ordered? true :expected [[3]]}
   {:name "ordering after pagination sorts the chosen subset"
    :expr ['-> division ['order-by [:item/id]] ['limit 2]
           ['order-by [[:item/id :desc]]]]
    :ordered? true :expected [[3] [1]]}
   {:name "zero limit after division"
    :expr ['limit division 0] :expected []}
   {:name "literal dots and slashes in attributes"
    :expr ['divide ['rename dividend [[:item/id :candidate.key/item-id]]] divisor]
    :expected [[1] [3] [8]]}
   {:name "renaming and projecting both operands"
    :expr ['divide
           ['rename ['project dividend [:item/id :tag/value]]
            [[:tag/value :required/tag]]]
           ['rename ['project divisor [:tag/value]]
            [[:tag/value :required/tag]]]]
    :expected [[1] [3] [4] [5] [7] [8]]}
   {:name "extension after a set-valued division"
    :expr ['extend division {:item/kind [:string "match"]}]
    :expected [[1 "match"] [3 "match"] [8 "match"]]}
   {:name "division composes with union"
    :expr ['union division division] :expected [[1] [3] [8]]}
   {:name "division composes with join"
    :expr ['join division ['rename division [[:item/id :other/id]]]
           '[= :item/id :other/id]]
    :expected [[1 1] [3 3] [8 8]]}
   {:name "required NULL value never matches"
    :expr ['divide dividend
           ['project ['restrict dividend '[= :item/id 6]]
            [:tag/value :tag/locale]]]
    :expected []}
   {:name "required NULL in another tuple component never matches"
    :expr ['divide dividend
           ['project ['restrict dividend '[= :item/id 7]]
            [:tag/value :tag/locale]]]
    :expected []}
   {:name "NULL candidate key cannot match a witness"
    :expr ['divide ['restrict dividend '[= :item/id nil]] divisor]
    :expected []}
   {:name "empty divisor preserves a NULL candidate key"
    :expr ['divide ['restrict dividend '[= :item/id nil]] empty-divisor]
    :expected [[nil]]}
   {:name "equal headings produce the empty tuple when requirements hold"
    :expr ['divide example-dividend example-dividend] :expected [[1]]}
   {:name "equal headings with an unsatisfied requirement"
    :expr ['divide example-dividend dividend] :expected []}
   {:name "empty divisor and zero result attributes"
    :expr ['divide divisor empty-divisor] :expected [[1]]}
   {:name "both empty with zero result attributes"
    :expr ['divide empty-divisor empty-divisor] :expected []}
   {:name "nonempty zero-attribute divisor"
    :expr ['divide ['project dividend [:item/id]] ['project divisor []]]
    :expected (vec (remove #{[nil]} all-ids))}
   {:name "empty zero-attribute divisor"
    :expr ['divide ['project dividend [:item/id]] ['project empty-divisor []]]
    :expected all-ids}
   {:name "both headings empty"
    :expr ['divide ['project dividend []] ['project divisor []]]
    :expected [[1]]}
   {:name "both headings and both relations empty"
    :expr ['divide ['project empty-dividend []] ['project empty-divisor []]]
    :expected []}
   {:name "empty dividend and nonempty divisor with empty headings"
    :expr ['divide ['project empty-dividend []] ['project divisor []]]
    :expected []}
   {:name "zero-attribute joins as division operands"
    :expr ['divide
           ['join ['project dividend []] ['project divisor []] '[= nil nil]]
           ['project divisor []]]
    :expected [[1]]}
   {:name "nested zero-attribute division"
    :expr ['divide division ['divide divisor empty-divisor]]
    :expected [[1] [3] [8]]}
   {:name "project away all attributes"
    :expr ['divide dividend ['project-away divisor [:tag/value :tag/locale]]]
    :expected [[1 "A" "en"] [1 "B" "en"] [2 "A" "en"]
               [3 "A" "en"] [3 "B" "en"] [3 "C" "en"]
               [4 "A" "fr"] [4 "B" "fr"] [5 "A" "en"] [5 "B" "fr"]
               [8 "A" "en"] [8 "A" "fr"] [8 "B" "en"] [8 "B" "fr"]]}
   {:name "extension of a zero-attribute result hides the unit column"
    :expr ['extend ['divide divisor empty-divisor] {:answer [:string "yes"]}]
    :expected [["yes"]]}
   {:name "empty extension preserves a zero-attribute result"
    :expr ['extend ['divide divisor empty-divisor] {}]
    :expected [[1]]}
   {:name "joining a zero-attribute quotient hides its unit column"
    :expr ['join ['divide divisor empty-divisor] division '[= nil nil]]
    :expected [[1] [3] [8]]}
   {:name "joining a zero-attribute quotient on the right hides its unit column"
    :expr ['join division ['divide divisor empty-divisor] '[= nil nil]]
    :expected [[1] [3] [8]]}])


(defn assert-division!
  [connection opts]
  (doseq [{:keys [name expr params expected ordered?]} cases]
    (testing (str (:db-type opts) " " name)
      (let [ast (raql/compile heading-relmap expr)
            attrs (if (seq (:heading ast))
                    (mapv :attr/name (:heading ast))
                    [:__raql_unit])
            honey (rh/transform ast (assoc opts :params (or params {})))
            statement (sql/format honey (merge
                                          {:dialect (pagination/dialect (:db-type opts))
                                           :params (or params {})}
                                          (:format-opts opts)))
            rows (jdbc/execute! connection statement
                                {:builder-fn rs/as-unqualified-maps})
            tuples (mapv #(mapv % attrs) rows)]
        (is (every? #(= (set attrs) (set (keys %))) rows))
        (is (= (if ordered? expected (frequencies expected))
               (if ordered? tuples (frequencies tuples))))))))


(defn with-database
  [db-type jdbc-url f]
  (let [prefix (str "raql_divide_" (str/replace (str (random-uuid)) "-" "") "_")
        table-name #(str prefix (str/replace (name %) "-" "_"))
        item-tags (str "\"" (table-name :item-tags) "\"")
        required-tags (str "\"" (table-name :required-tags) "\"")
        created (atom [])]
    (with-open [connection (jdbc/get-connection jdbc-url)]
      (when (= :mssql db-type)
        (jdbc/execute! connection ["SET QUOTED_IDENTIFIER ON"]))
      (try
        (jdbc/execute! connection
                       [(str "CREATE TABLE " item-tags
                             " (\"id\" INTEGER, \"value\" VARCHAR(40), \"locale\" VARCHAR(40))")])
        (swap! created conj item-tags)
        (jdbc/execute! connection
                       [(str "CREATE TABLE " required-tags
                             " (\"value\" VARCHAR(40), \"locale\" VARCHAR(40))")])
        (swap! created conj required-tags)
        (doseq [[id tag locale] [[1 "A" "en"] [1 "B" "en"] [1 "A" "en"]
                                 [2 "A" "en"]
                                 [3 "A" "en"] [3 "B" "en"] [3 "C" "en"] [3 "B" "en"]
                                 [4 "A" "fr"] [4 "B" "fr"]
                                 [5 "A" "en"] [5 "B" "fr"]
                                 [6 nil "en"]
                                 [nil "A" "en"] [nil "B" "en"]
                                 [7 "A" nil] [7 "B" nil]
                                 [8 "A" "en"] [8 "A" "fr"] [8 "B" "en"] [8 "B" "fr"]]]
          (jdbc/execute! connection
                         [(str "INSERT INTO " item-tags " VALUES (?, ?, ?)") id tag locale]))
        (doseq [[tag locale] [["A" "en"] ["B" "en"] ["A" "en"]]]
          (jdbc/execute! connection
                         [(str "INSERT INTO " required-tags " VALUES (?, ?)") tag locale]))
        (f connection {:db-type db-type :table-name-fn table-name})
        (finally
          (doseq [table (reverse @created)]
            (jdbc/execute! connection [(str "DROP TABLE " table)])))))))
