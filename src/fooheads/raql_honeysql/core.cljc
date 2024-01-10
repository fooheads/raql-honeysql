(ns fooheads.raql-honeysql.core
  (:refer-clojure :exclude [-' +' *'])
  (:require
    [clojure.walk :as walk]
    [fooheads.raql.ast :as ast]
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
  [node]
  (let [attr-names (heading-attr-names node)
        sql-names (map (comp vector sql-attr-name) attr-names)
        attr-unqualified-names (map (comp keyword name) attr-names)
        selection (mapv vector attr-unqualified-names sql-names)]
    selection))


(defn- relation'
  ([node opts]
   (let [relvar-name (sql-relation-name (:db-schema opts) (first (:args node)))]
     (assoc node :honey {:select (selection node)
                         :from (from relvar-name)})))

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


(defn- limit'
  [node _opts]
  (let [[rel limit offset] (:args node)]
    (assoc node :honey {:select :*
                        :from (from (:honey rel))
                        :limit limit
                        :offset (or offset 0)})))


(defn- join'
  [join-op node opts]
  (let [[xrel yrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from [[(:honey xrel) :__xrel]]
                        join-op [[(:honey yrel) :__yrel]
                                 (raw-restriction restriction opts)]})))


(defn- union'
  [node _opts]
  (let [[xrel yrel] (:args node)]
    (assoc node :honey {:union [(:honey xrel) (:honey yrel)]})))


(defn- distinct'
  [node _opts]
  (let [[xrel] (:args node)]
    (assoc node :honey {:select-distinct :* :from (from (:honey xrel))})))


(defn- order-by'
  [node _opts]
  (let [[rel ordering] (:args node)]
    (assoc
      node
      :honey
      (merge (:honey rel) {:order-by (mapv (fn [attr]
                                             (if (vector? attr)
                                               (let [[attr-name order] attr]
                                                 [(sql-attr-name attr-name) order])
                                               [(sql-attr-name attr) :asc]))

                                           ordering)}))))


(def env
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
   'limit limit'
   'union union'
   'order-by order-by'})


(defn transform
  ([ast]
   (transform ast {}))
  ([ast opts]
   (:honey
     (walk/postwalk
       (apply-if
         ast/node?
         (fn [node]
           (let [operator (:operator node)
                 f (env operator)]
             (f node opts))))

       ast))))

