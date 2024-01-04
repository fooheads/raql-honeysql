(ns fooheads.raql-honeysql.core
  (:refer-clojure :exclude [-' +' *'])
  (:require
    [clojure.walk :as walk]
    [fooheads.raql.ast :as ast]
    [fooheads.stdlib :refer [apply-if qualified-name throw-ex]]))


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


(defn- raw-restriction
  [restriction]
  (walk/postwalk (apply-if qualified-keyword? sql-attr-name) restriction))


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
  ([node]
   (assoc node :honey {:select (selection node)
                       :from [[(first (:args node)) :__relation]]}))

  ;; Not yet implemented
  #_([namn projection]
     {:select (attrs-projection projection)
      :from [[(keyword namn) :__relation]]}))


(defn- rename'
  [node]
  (let [[xrel renames] (:args node)
        underlying-attr-names (->> xrel :heading (map :attr/name))
        attrs-to-rename (set (map first renames))
        attrs-to-select (remove attrs-to-rename underlying-attr-names)
        attrs-pairs (map vector attrs-to-select attrs-to-select)
        selection (concat attrs-pairs renames)]

    (assoc node :honey {:select (raw-selection selection)
                        :from (:honey xrel)})))


(defn- project'
  [node]
  (let [[xrel projection] (:args node)]
    (assoc node :honey {:select (raw-selection projection)
                        :from (:honey xrel)})))


(defn- project-away'
  [node]
  (let [[xrel anti-projection] (:args node)
        anti-projection (set anti-projection)
        attr-names (->> xrel :heading (map :attr/name))
        projection (remove anti-projection attr-names)]
    (assoc node :honey {:select (raw-selection projection)
                        :from (:honey xrel)})))


(defn- restrict'
  [node]
  (let [[xrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from (:honey xrel)
                        :where (raw-restriction restriction)})))


(defn- limit'
  [node]
  (let [[rel limit offset] (:args node)]
    (assoc node :honey {:select :* :from (:honey rel) :limit limit :offset (or offset 0)})))


(defn- join'
  [join-op node]
  (let [[xrel yrel restriction] (:args node)]
    (assoc node :honey {:select :*
                        :from [[(:honey xrel) :__xrel]]
                        join-op [[(:honey yrel) :__yrel]
                                 (raw-restriction restriction)]})))


(defn- union'
  [node]
  (let [[xrel yrel] (:args node)]
    (assoc node :honey {:union [(:honey xrel) (:honey yrel)]})))


(defn- distinct'
  [node]
  (let [[xrel] (:args node)]
    (assoc node :honey {:select-distinct :* :from (:honey xrel)})))


(defn- order-by'
  [node]
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
  [ast]
  (:honey
    (walk/postwalk
      (apply-if
        ast/node?
        (fn [node]
          (let [operator (:operator node)
                f (env operator)]
            (f node))))

      ast)))

