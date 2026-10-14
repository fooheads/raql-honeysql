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


(defn- raw-selection
  "Takes a list of attr-names (namespaced keywords) or pairs of attr-names and
  turns them into 'raw' honey."
  [attr-name-pairs]
  (mapv
    (fn [x]
      (cond
        (pair? x)
        [[(sql-attr-name (first x))] [(sql-attr-name (second x))]]

        (keyword? x)
        [(sql-attr-name x)]

        :else
        (throw-ex "Not a valid attr-name or pair: {x}")))
    attr-name-pairs))


(defn- from
  ([relvar-name]
   (from relvar-name :__relation))
  ([relvar-name as]
   [[relvar-name as]]))


(defn- raw-restriction
  [restriction opts]
  (let [[operator & operands] restriction]
    (case operator
      in
      (let [[attr-name vector-or-subquery] operands]
        (cond
          (map? vector-or-subquery)
          ['in (sql-attr-name attr-name) (transform vector-or-subquery opts)]

          :else
          (walk/postwalk (apply-if qualified-keyword? sql-attr-name) restriction)))

      (walk/postwalk (apply-if qualified-keyword? sql-attr-name) restriction))))


(defn- heading-attr-names
  [node]
  (->> node :heading (mapv :attr/name)))


(defn- selection
  [node column-name-fn]
  (let [column-name (fn [attr] (column-name-fn (:attr/relvar-name attr) (:attr/name attr)))
        attr-names (heading-attr-names node)
        sql-names (map (comp vector sql-attr-name) attr-names)
        attr-unqualified-names (map (comp sql-attr-name column-name) (:heading node))
        selection (mapv vector attr-unqualified-names sql-names)]
    selection))


(defn- relation'
  ([node {:keys [table-name-fn column-name-fn db-schema]}]
   (let [relvar-name (first (:args node))
         table-name (table-name-fn relvar-name)
         namespaced-table-name (sql-relation-name db-schema table-name)]
     (assoc node :honey {:select (selection node column-name-fn)
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
    (assoc node :honey {:select (raw-selection projection)
                        :from (from (:honey xrel))})))


(defn- project-away'
  [node _opts]
  (let [[xrel anti-projection] (:args node)
        anti-projection (set anti-projection)
        attr-names (->> xrel :heading (map :attr/name))
        projection (remove anti-projection attr-names)]
    (assoc node :honey {:select (raw-selection projection)
                        :from (from (:honey xrel))})))


(defn- restrict'
  [node opts]
  (let [[xrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from (from (:honey xrel))
                        :where (raw-restriction restriction opts)})))


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
        :postgresql
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
  (let [[xrel yrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from [[(:honey xrel) :__xrel]]
                        join-op [[(:honey yrel) :__yrel]
                                 (raw-restriction restriction opts)]})))


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
        honey (if (::window rel)
                {:select (raw-selection (heading-attr-names rel))
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

  :db-type selects :postgresql (default), :mssql, or :hsqldb.
  :params supplies named pagination values; pass it to HoneySQL formatting
  as well for restriction parameters. SQL Server offsets require order-by."
  ([ast]
   (transform ast {}))
  ([ast opts]
   (let [opts (merge default-opts opts)]
     (when-not (#{:postgresql :mssql :hsqldb} (:db-type opts))
       (throw (ex-info "Unsupported pagination database type"
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
