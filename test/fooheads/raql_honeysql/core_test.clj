(ns fooheads.raql-honeysql.core-test
  (:require
    [chinook]
    [clojure.test :refer [are deftest is]]
    [fooheads.raql-honeysql.chinook-schema :as chinook-schema]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql.core :as raql]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(def builder rs/as-unqualified-maps)
(def heading-relmap (chinook-schema/heading-relmap))
(def heading-relmap-with-schema (chinook-schema/heading-relmap))


(defn honey
  [raql]
  (let [ast (raql/compile heading-relmap {} raql)]
    (rh/transform ast)))


(defn sql
  [expr]
  (sql/format (honey expr)))


(defn raql!
  "Executes a RAQL expression"
  [expr]
  (jdbc/execute! chinook/ds (sql expr)
                 {:builder-fn builder}))


(defn sql!
  "Executes a SQL expression"
  [expr]
  (jdbc/execute! chinook/ds expr
                 {:builder-fn builder}))


(defn honey!
  "Executes a HoneySQL expression"
  [expr]
  (-> expr (sql/format) (sql!)))


(deftest honey-test
  (let [raql '[->
               [relation :Artist]
               [project [:Artist/Name]]
               [restrict [= :Artist/Name "Jimi Hendrix"]]]
        ast (raql/compile heading-relmap {} raql)]
    (->
      ast
      (rh/transform)
      (sql/format))))


(deftest transform-test
  (are [x y] (= x y)

    ;; scan
    (raql! '[relation :Artist])
    (sql!   ["select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\""])


    ;; rename
    (raql! '[->
             [relation :Artist]
             [rename [[:Artist/ArtistId :Artist/SomeId]]]])

    (sql!   ["select
                \"Artist/ArtistId\" as \"Artist/SomeId\",
                \"Artist/Name\" as \"Artist/Name\"
              from
              (select
                 \"ArtistId\" as \"Artist/ArtistId\",
                 \"Name\" as \"Artist/Name\"
               from \"Artist\")"])

    ;; project
    (raql! '[->
             [relation :Artist]
             [project [:Artist/Name]]])

    (sql!   ["select
                \"Artist/Name\" as \"Artist/Name\"
              from
              (select
                 \"ArtistId\" as \"Artist/ArtistId\",
                 \"Name\" as \"Artist/Name\"
               from \"Artist\")"])

    ;; project-away
    (raql! '[->
             [relation :Artist]
             [project-away [:Artist/ArtistId]]])

    (sql!   ["select
                \"Artist/Name\" as \"Artist/Name\"
              from
              (select
                 \"ArtistId\" as \"Artist/ArtistId\",
                 \"Name\" as \"Artist/Name\"
               from \"Artist\")"])

    ;; restrict
    (raql! '[->
             [relation :Artist]
             [restrict [= :Artist/ArtistId 5]]])

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\")
             where \"Artist/ArtistId\" = 5"])

    (raql! '[->
             [relation :Artist]
             [restrict
              [or
               [= :Artist/ArtistId 5]
               [= :Artist/ArtistId 6]]]])

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\")
             where \"Artist/ArtistId\" = 5
             or    \"Artist/ArtistId\" = 6"])

    ;; distinct
    (raql! '[->
             [relation :Track]
             [project [:Track/Composer]]
             [distinct]])

    (sql!   ["select
                 distinct
                 \"Composer\" as \"Track/Composer\"
               from \"Track\""])

    ;; limit
    (raql! '[->
             [relation :Artist]
             [limit 5]])

    (sql!   ["select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\"
              limit 5"])

    ;; limit and offset
    (raql! '[->
             [relation :Artist]
             [limit 5 5]])

    (sql!   ["select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\"
              limit 5
              offset 5"])

    ;; join
    (raql! '[->
             [relation :Artist]
             [join
              [relation :Album]
              [= :Artist/ArtistId :Album/ArtistId]]])

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\") as \"Artist\"
             join
             (select
                \"AlbumId\" as \"Album/AlbumId\",
                \"Title\" as \"Album/Title\",
                \"ArtistId\" as \"Album/ArtistId\"
              from \"Album\") as \"Album\"
             on \"Artist/ArtistId\" = \"Album/ArtistId\""])


    ;; full join
    (raql! '[->
             [project
              [relation :Track]
              [:Track/TrackId :Track/Name]]
             [full-join
              [project
               [relation :InvoiceLine]
               [:InvoiceLine/InvoiceId :InvoiceLine/TrackId]]
              [= :Track/TrackId :InvoiceLine/TrackId]]])

    (sql!   ["select *
             from
             (select
                \"TrackId\" as \"Track/TrackId\",
                \"Name\" as \"Track/Name\"
              from \"Track\") as \"Track\"

              full outer join

              (select
                 \"InvoiceId\" as \"InvoiceLine/InvoiceId\",
                 \"TrackId\"   as \"InvoiceLine/TrackId\"
               from \"InvoiceLine\") as \"InvoiceLine\"

              on \"Track/TrackId\" = \"InvoiceLine/TrackId\"
             "])


    ;; left join
    (raql! '[->
             [project
              [relation :Track]
              [:Track/TrackId :Track/Name]]
             [left-join
              [project
               [relation :InvoiceLine]
               [:InvoiceLine/InvoiceId :InvoiceLine/TrackId]]
              [= :Track/TrackId :InvoiceLine/TrackId]]])

    (sql!   ["select *
             from
             (select
                \"TrackId\" as \"Track/TrackId\",
                \"Name\" as \"Track/Name\"
              from \"Track\") as \"Track\"

              left join

              (select
                 \"InvoiceId\" as \"InvoiceLine/InvoiceId\",
                 \"TrackId\"   as \"InvoiceLine/TrackId\"
               from \"InvoiceLine\") as \"InvoiceLine\"

              on \"Track/TrackId\" = \"InvoiceLine/TrackId\"
             "])


    ;; right join
    (raql! '[->
             [project
              [relation :InvoiceLine]
              [:InvoiceLine/InvoiceId :InvoiceLine/TrackId]]
             [right-join
              [project
               [relation :Track]
               [:Track/TrackId :Track/Name]]
              [= :Track/TrackId :InvoiceLine/TrackId]]])

    (sql!   ["select *
             from
              (select
                 \"InvoiceId\" as \"InvoiceLine/InvoiceId\",
                 \"TrackId\"   as \"InvoiceLine/TrackId\"
               from \"InvoiceLine\") as \"InvoiceLine\"

              right join

             (select
                \"TrackId\" as \"Track/TrackId\",
                \"Name\" as \"Track/Name\"
              from \"Track\") as \"Track\"

              on \"InvoiceLine/TrackId\" = \"Track/TrackId\"
             "])

    ;; union
    (raql! '[union
             [restrict
              [relation :Artist]
              [or
               [= :Artist/ArtistId 5]
               [= :Artist/ArtistId 6]]]
             [restrict
              [relation :Artist]
              [or
               [= :Artist/ArtistId 6]
               [= :Artist/ArtistId 7]]]])

    (sql!   ["select *
                from
                (select
                   \"ArtistId\" as \"Artist/ArtistId\",
                   \"Name\" as \"Artist/Name\"
                 from \"Artist\")
                where \"Artist/ArtistId\" = 5 or
                      \"Artist/ArtistId\" = 6

              union

              select *
               from
               (select
                  \"ArtistId\" as \"Artist/ArtistId\",
                  \"Name\" as \"Artist/Name\"
                from \"Artist\")
                where \"Artist/ArtistId\" = 6 or
                      \"Artist/ArtistId\" = 7
             "])


    ;; order-by
    (raql! '[->
             [join
              [relation :Artist]
              [relation :Album]
              [= :Artist/ArtistId :Album/ArtistId]]
             [order-by [[:Artist/Name :desc] :Album/Title]]])

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\") as \"Artist\"
             join
             (select
                \"AlbumId\" as \"Album/AlbumId\",
                \"Title\" as \"Album/Title\",
                \"ArtistId\" as \"Album/ArtistId\"
              from \"Album\") as \"Album\"
             on \"Artist/ArtistId\" = \"Album/ArtistId\"
             order by \"Artist/Name\" desc, \"Album/Title\""])))


(deftest transform-with-namespace-test
  (let [raql '[relation :Artist]
        ast (raql/compile heading-relmap {} raql)]
    (is (= [[[:raw "\"Artist\""] :__relation]]
           (:from
             (as->
               ast $
               (rh/transform $ {})))))

    (is (= [[[:raw "\"chinook\".\"Artist\""] :__relation]]
           (:from (as->
                    ast $
                    (rh/transform $ {:db-schema "chinook"})))))))


(comment
  (->
    '[restrict
      [relation :Artist]
      [= :Artist/Name ?name]]
    (honey)
    (sql/format {:params {:name "Jimi Hendrix"}})))

