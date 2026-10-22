(ns fooheads.raql-honeysql.core
  (:refer-clojure :exclude [-' +' *'])
  (:require
    [clojure.walk :as walk]
    [fooheads.raql.ast :as ast]
    [fooheads.raql.pagination :as pagination]
    [fooheads.stdlib :refer [apply-if qualified-name throw-ex]]))


(declare transform)


(defn- sql-relation-name
  ([kw]
   [:raw (str "\"" (name kw) "\"")])
  ([db-schema kw]
   (if db-schema
     [:raw (str (str "\"" (name db-schema) "\"")
                "."
                (str "\"" (name kw) "\""))]
     (sql-relation-name kw))))


(defn- sql-attr-name
  [kw]
  [:raw (str "\"" (qualified-name kw) "\"")])


(defn- pair?
  [x]
  (and (vector? x) (= 2 (count x))))


(def ^:private unit-selection
  [[[:inline 1] [(sql-attr-name :__raql_unit)]]])


(defn- raw-selection
  "Takes a list of attr-names (namespaced keywords) or pairs of attr-names and
  turns them into 'raw' honey."
  [attr-name-pairs]
  (if (seq attr-name-pairs)
    (mapv
      (fn [x]
        (cond
          (pair? x)
          [[(sql-attr-name (first x))] [(sql-attr-name (second x))]]

          (keyword? x)
          [(sql-attr-name x)]

          :else
          (throw-ex "Not a valid attr-name or pair: {x}")))
      attr-name-pairs)
    unit-selection))


(defn- from
  ([relvar-name]
   (from relvar-name :__relation))
  ([relvar-name as]
   [[relvar-name as]]))


(defn- raw-restriction
  [restriction opts]
  (let [[operator & operands] (when (sequential? restriction) restriction)
        attribute? #(or (qualified-keyword? %) (contains? (::attributes opts) %))]
    (case operator
      (and or not)
      (into [operator] (map #(raw-restriction % opts) operands))

      in
      (let [[attr-name vector-or-subquery] operands]
        (cond
          (and (ast/node? vector-or-subquery)
               (ast/operator? (:operator vector-or-subquery)))
          ['in (sql-attr-name attr-name) (transform vector-or-subquery opts)]

          :else
          (walk/postwalk (apply-if attribute? sql-attr-name) restriction)))

      (walk/postwalk (apply-if attribute? sql-attr-name) restriction))))


(defn- heading-attr-names
  [node]
  (->> node :heading (mapv :attr/name)))


(defn- selection
  [node column-name-fn]
  (let [column-name (fn [attr] (column-name-fn (:attr/relvar-name attr) (:attr/name attr)))
        attr-names (heading-attr-names node)
        sql-names (map (comp vector sql-attr-name) attr-names)
        attr-unqualified-names (map (comp sql-attr-name column-name) (:heading node))
        selection (if (seq attr-names)
                    (mapv vector attr-unqualified-names sql-names)
                    unit-selection)]
    selection))


(defn- relation'
  ([node {:keys [table-name-fn column-name-fn db-schema]}]
   (let [relvar-name (first (:args node))
         table-name (table-name-fn relvar-name)
         namespaced-table-name (sql-relation-name db-schema table-name)]
     (assoc node :honey {(if (seq (:heading node)) :select :select-distinct)
                         (selection node column-name-fn)
                         :from (from namespaced-table-name)})))

  ;; Not yet implemented
  #_([namn projection]
     {:select (attrs-projection projection)
      :from (from (keyword namn))}))


(defn- rename'
  [node _opts]
  (let [[xrel renames] (:args node)
        underlying-attr-names (->> xrel :heading (map :attr/name))
        attrs-to-rename (set (map first renames))
        attrs-to-select (remove attrs-to-rename underlying-attr-names)
        attrs-pairs (map vector attrs-to-select attrs-to-select)
        selection (concat attrs-pairs renames)]

    (assoc node :honey {:select (raw-selection selection)
                        :from (from (:honey xrel))})))


(defn- project'
  [node _opts]
  (let [[xrel projection] (:args node)]
    (assoc node :honey {(if (seq projection) :select :select-distinct)
                        (raw-selection projection)
                        :from (from (:honey xrel))})))


(defn- project-away'
  [node _opts]
  (let [[xrel anti-projection] (:args node)
        anti-projection (set anti-projection)
        attr-names (->> xrel :heading (map :attr/name))
        projection (remove anti-projection attr-names)]
    (assoc node :honey {(if (seq projection) :select :select-distinct)
                        (raw-selection projection)
                        :from (from (:honey xrel))})))


(defn- restrict'
  [node opts]
  (let [[xrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from (from (:honey xrel))
                        :where (raw-restriction
                                 restriction
                                 (assoc opts ::attributes (set (heading-attr-names xrel))))})))


(defn- offset-window
  [window n]
  (cond-> (-> window
              (update :offset + n)
              (assoc :offset? true))
    (some? (:limit window)) (update :limit #(max 0 (- % n)))))


(defn- limit-window
  [window n]
  (update window :limit #(if (some? %) (min % n) n)))


(defn- pagination-honey
  [{:keys [source order-by offset offset? limit]} db-type]
  (when (and (= :mssql db-type) offset? (not (seq order-by)))
    (throw (ex-info "SQL Server offset requires an explicit order-by"
                    {:db-type db-type :offset offset :limit limit})))
  (let [honey (cond-> {:select :* :from (from source)}
                (seq order-by) (assoc :order-by order-by))]
    (if (= 0 limit)
      (assoc (dissoc honey :order-by) :where [:= [:inline 1] [:inline 0]])
      (case db-type
        (:postgresql :sqlite)
        (cond-> honey
          (some? limit) (assoc :limit limit)
          offset? (assoc :offset offset))

        :mssql
        (if offset?
          (cond-> (assoc honey :offset offset)
            (some? limit) (assoc :fetch limit))
          (-> honey
              (dissoc :select)
              (assoc :select-top [limit :*])))

        :hsqldb
        (cond-> honey
          (some? limit) (assoc :fetch limit)
          offset? (assoc :offset offset))))))


(defn- pagination'
  [node {:keys [params db-type]}]
  (let [operator (:operator node)
        args (:args node)
        _ (pagination/validate-args! operator args)
        [rel & values] args
        counts (mapv #(pagination/resolve-count % params) values)
        honey (:honey rel)
        window (or (::window rel)
                   {:source (dissoc honey :order-by)
                    :order-by (:order-by honey)
                    :offset 0
                    :offset? false
                    :limit nil})
        window (case operator
                 offset (offset-window window (first counts))
                 limit (limit-window
                         (if (= 2 (count counts))
                           (offset-window window (second counts))
                           window)
                         (first counts)))]
    (assoc node
           ::window window
           :honey (pagination-honey window db-type))))


(defn- join'
  [join-op node opts]
  (let [[xrel yrel restriction] (:args node)
        unit? (or (empty? (:heading xrel)) (empty? (:heading yrel)))
        xalias (if unit? :raql_xrel :__xrel)
        yalias (if unit? :raql_yrel :__yrel)
        selection (cond
                    (empty? (:heading node)) {:select-distinct unit-selection}
                    (empty? (:heading xrel)) {:select [:raql_yrel.*]}
                    (empty? (:heading yrel)) {:select [:raql_xrel.*]}
                    :else {:select :*})]
    (assoc node :honey (assoc selection
                              :from [[(:honey xrel) xalias]]
                              join-op [[(:honey yrel) yalias]
                                       (raw-restriction
                                         restriction
                                         (assoc opts ::attributes
                                                (set (concat (heading-attr-names xrel)
                                                             (heading-attr-names yrel)))))]))))


(defn- union'
  [node _opts]
  (let [operand (fn [rel]
                  (let [honey (:honey rel)]
                    (if (or (::window rel) (seq (:order-by honey)))
                      {:select :* :from (from honey)}
                      honey)))]
    (assoc node :honey {:union (mapv operand (:args node))})))


(defn- distinct'
  [node _opts]
  (let [[xrel] (:args node)]
    (assoc node :honey {:select-distinct :* :from (from (:honey xrel))})))


(defn- sql-number-type
  [type db-type]
  (case type
    :integer :bigint
    :double (case db-type :mssql :float :hsqldb :double :double-precision)))


(defn- sql-count
  [db-type expr]
  [(if (= :mssql db-type) :count_big :count) expr])


(defn- finite-number
  [expr]
  [:between expr [:inline -1.7976931348623157E308] [:inline 1.7976931348623157E308]])


(defn- aggregate'
  [node {:keys [db-type] :as opts}]
  (let [[rel aggregates] (:args node)
        types (into {} (map (juxt :attr/name :attr/type)) (:heading rel))
        result-types (into {} (map (juxt :attr/name :attr/type)) (:heading node))
        specs (mapv (fn [i [output [operator selector]]]
                      {:output output :operator operator :selector selector
                       :type (result-types output)
                       :value (str "raql_aggregate_value_" i)
                       :valid (str "raql_aggregate_valid_" i)})
                    (range) aggregates)
        numeric (filterv #(not= 'count (:operator %)) specs)
        _ (when (and (= :sqlite db-type) (seq numeric))
            (throw (ex-info
                     "SQLite cannot enforce strict numeric aggregate errors with scalar subqueries"
                     {:db-type db-type :capability :scalar-subquery-cardinality
                      :aggregates (mapv :operator numeric)})))
        id (swap! (::alias-counter opts) inc)
        stats-alias (keyword (str "raql_aggregate_stats_" id))
        check-alias (keyword (str "raql_aggregate_check_" id))
        column (fn [name] [:. stats-alias name])
        count-name "raql_aggregate_count"
        n (column count-name)
        source (cond-> (:honey rel)
                 (not (::window rel)) (dissoc :order-by))
        input {:select-distinct (raw-selection (heading-attr-names rel))
               :from (from source)}
        statistics (mapcat
                     (fn [{:keys [operator selector type value valid]}]
                       (let [attr (sql-attr-name selector)
                             sql-type (sql-number-type type db-type)
                             expr [(case operator sum :sum avg :avg)
                                   [:cast attr sql-type]]
                             result (if (= 'sum operator)
                                      [:case
                                       [:= (sql-count db-type :*) [:inline 0]]
                                       [:cast [:inline 0] sql-type]
                                       :else [:cast expr sql-type]]
                                      expr)
                             present (if (= :double (types selector))
                                       [:case (finite-number attr) [:inline 1] :else nil]
                                       attr)]
                         [[result value] [(sql-count db-type present) valid]]))
                     numeric)
        stats {:select (into [[[:cast (sql-count db-type :*) :bigint] count-name]]
                             statistics)
               :from (from input)}
        invalid (cons [:= n nil]
                      (mapcat
                        (fn [{:keys [operator type value valid]}]
                          (cond-> [[:not= (column valid) n]
                                   [:= (column value) nil]]
                            (= 'avg operator) (conj [:= n [:inline 0]])
                            (= :double type)
                            (conj [:= [:case (finite-number (column value)) [:inline 0]
                                       :else [:inline 1]]
                                   [:inline 1]])))
                        numeric))
        ;; A scalar subquery must produce at most one row. The second row
        ;; makes invalid aggregates raise a SQL-standard cardinality violation.
        check {:select [[[:inline 1]]]
               :from [[{:values [[[:inline 0]] [[:inline 1]]]}
                       [check-alias {:columns [:candidate]}]]]
               :where (into [:or [:= [:. check-alias :candidate] [:inline 0]]] invalid)}
        selection (if (seq specs)
                    (mapv (fn [{:keys [output operator value]}]
                            [[(if (= 'count operator) n (column value))]
                             [(sql-attr-name output)]])
                          specs)
                    unit-selection)]
    (assoc node :honey
           (cond-> {:select selection :from (from stats stats-alias)}
             (or (seq numeric) (= :mssql db-type))
             (assoc :where [:= check [:inline 1]])))))


(defn- divide'
  [node opts]
  (let [[dividend divisor] (:args node)
        id (swap! (::alias-counter opts) inc)
        ;; HSQLDB rejects underscore-prefixed qualifiers when quoting is off.
        candidate (keyword (str "raql_divide_candidate_" id))
        required (keyword (str "raql_divide_required_" id))
        witness (keyword (str "raql_divide_witness_" id))
        columns (into {}
                      (map-indexed
                        (fn [i attr]
                          [(:attr/name attr) (keyword (str "__divide_attr_" i))])
                        (:heading dividend)))
        column (fn [alias attr] [:. alias (name (columns attr))])
        ;; Internal column names keep literal RAQL names (including / and .)
        ;; out of HoneySQL's qualified-identifier splitting.
        operand (fn [rel]
                  {:select (raw-selection
                             (mapv (fn [attr] [attr (columns attr)])
                                   (heading-attr-names rel)))
                   :from (from (:honey rel))})
        dividend-honey (operand dividend)
        result-attrs (heading-attr-names node)
        matches (concat
                  (map (fn [attr]
                         [:= (column witness attr) (column candidate attr)])
                       result-attrs)
                  (map (fn [attr]
                         [:= (column witness attr) (column required attr)])
                       (heading-attr-names divisor)))
        match-query {:select [[[:inline 1]]]
                     :from (from dividend-honey witness)
                     :where (if (seq matches)
                              (into [:and] matches)
                              [:= [:inline 1] [:inline 1]])}
        missing-query {:select [[[:inline 1]]]
                       :from (from (operand divisor) required)
                       :where [:not [:exists match-query]]}]
    (assoc node :honey
           {:select-distinct
            (if (seq result-attrs)
              (mapv (fn [attr]
                      [[(column candidate attr)] [(sql-attr-name attr)]])
                    result-attrs)
              unit-selection)
            :from (from dividend-honey candidate)
            :where [:not [:exists missing-query]]})))


(defn- order-by'
  [node _opts]
  (let [[rel ordering] (:args node)
        honey (if (::window rel)
                {:select :* :from (from (:honey rel))}
                (:honey rel))]
    (assoc
      node
      :honey
      (merge honey {:order-by (mapv (fn [attr]
                                      (if (vector? attr)
                                        (let [[attr-name order] attr]
                                          [(sql-attr-name attr-name) order])
                                        [(sql-attr-name attr) :asc]))

                                    ordering)}))))


(defn- constant-extension
  [[attr-name [_type constant]]]
  (let [sql-name [(sql-attr-name attr-name)]]
    [constant sql-name]))


(defn- extend'
  [node _opts]
  (let [[rel extensions] (:args node)
        honey (if (or (::window rel)
                      (:select-distinct (:honey rel))
                      (empty? (:heading rel)))
                {:select (if (seq (:heading rel))
                           (raw-selection (heading-attr-names rel))
                           (if (seq extensions) [] unit-selection))
                 :from (from (:honey rel))}
                (:honey rel))]
    (assoc node :honey
           (update honey :select
                   #(into (if (= :* %) (raw-selection (heading-attr-names rel)) %)
                          (mapv constant-extension extensions))))))


(def ^:private env
  {'relation relation'
   'rename rename'
   'project project'
   'project-away project-away'
   'restrict restrict'
   'distinct distinct'
   'join       (partial join' :join)
   'full-join  (partial join' :full-join)
   'left-join  (partial join' :left-join)
   'right-join (partial join' :right-join)
   'limit pagination'
   'offset pagination'
   'union union'
   'divide divide'
   'aggregate aggregate'
   'order-by order-by'
   'extend extend'})


(def default-opts
  {:db-type :postgresql
   :table-name-fn name
   :column-name-fn (fn [_relvar-name attr-name] (name attr-name))})


(defn- validate-sqlserver-ordering!
  [honey]
  (letfn [(visit
            [value nested?]
            (cond
              (map? value)
              (do
                (when (and nested? (seq (:order-by value))
                           (not-any? #(contains? value %)
                                     [:offset :select-top :select-distinct-top]))
                  (throw (ex-info
                           "SQL Server subquery order-by must be paired with pagination"
                           {:db-type :mssql :query value})))
                (doseq [child (vals value)]
                  (visit child true)))

              (sequential? value)
              (doseq [child value]
                (visit child nested?))))]
    (visit honey false)))


(defn transform
  "Transforms a compiled RAQL AST into HoneySQL.

  :db-type selects :postgresql (default), :mssql, :hsqldb, or :sqlite.
  :params supplies named pagination values; pass it to HoneySQL formatting
  as well for restriction parameters. SQL Server offsets require order-by.
  Division uses SQL equality; zero-attribute results use __raql_unit.
  SQLite supports aggregate count, but not strict sum or avg."
  ([ast]
   (transform ast {}))
  ([ast opts]
   (let [opts (-> (merge default-opts opts)
                  (update ::alias-counter #(or % (atom 0))))]
     (when-not (#{:postgresql :mssql :hsqldb :sqlite} (:db-type opts))
       (throw (ex-info "Unsupported database type"
                       {:db-type (:db-type opts)})))
     (when (and (contains? opts :params) (not (map? (:params opts))))
       (throw (ex-info "Transform :params must be a map" {:params (:params opts)})))
     (let [honey
           (:honey
             (walk/postwalk
               (apply-if
                 ast/node?
                 (fn [node]
                   (let [operator (:operator node)
                         f (env operator)]
                     (when-not f
                       (throw (ex-info "Unsupported RAQL operator" {:operator operator})))
                     (f node opts))))
               ast))]
       (when (= :mssql (:db-type opts))
         (validate-sqlserver-ordering! honey))
       honey))))
