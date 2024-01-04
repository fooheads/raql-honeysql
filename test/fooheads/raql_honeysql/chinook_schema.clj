(ns fooheads.raql-honeysql.chinook-schema
  "A copy of chinook-schema from fooheads.raql. Would be nice to find
  a common place for this"
  (:require
    [camel-snake-kebab.core :as csk]
    [chinook.schema :as chinook-schema]
    [fooheads.setish :as set]
    [fooheads.stdlib :refer [map-vals qualify-ident]]))


(defn map-rel-vals
  [f rel]
  (map #(map-vals f %) rel))


(defn kebab-kw
  [x]
  (keyword (csk/->kebab-case x)))


(defn heading-relmap
  []
  (let [columns (map-rel-vals keyword (:column (chinook-schema/schema)))

        attrs
        (map
          (fn [column]
            {:attr/name (qualify-ident
                          (:column/table-name column)
                          (:column/name column))
             :attr/type (:column/type column)
             :attr/relvar-name (:column/table-name column)})
          columns)

        relvars
        (->
          columns
          (set/project [:column/table-name])
          (set/rename {:column/table-name :relvar/name}))]

    {:attr (vec attrs) :relvar (vec relvars)}))


(comment
  (heading-relmap))

