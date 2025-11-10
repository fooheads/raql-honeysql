(ns fooheads.raql-honeysql.core-test
  (:require
    [camel-snake-kebab.core :refer [->kebab-case]]
    [chinook]
    [clojure.string :as str]
    [clojure.test :refer [are deftest is]]
    [clojure.walk :refer [postwalk]]
    [fooheads.raql-honeysql.chinook-schema :as chinook-schema]
    [fooheads.raql-honeysql.core :as rh]
    [fooheads.raql.core :as raql]
    [fooheads.stdlib :refer [apply-if]]
    [honey.sql :as sql]
    [next.jdbc :as jdbc]
    [next.jdbc.result-set :as rs]))


(defn kebab-case-keyword
  [kw]
  (let [nspace (namespace kw)
        namn (name kw)]
    (if nspace
      (keyword (->kebab-case nspace) (->kebab-case namn))
      (keyword (->kebab-case namn)))))


(def builder rs/as-unqualified-maps)
(def heading-relmap (chinook-schema/heading-relmap))


(def kebab-heading-relmap
  (postwalk (apply-if keyword? kebab-case-keyword) heading-relmap))


(defn honey
  [raql]
  (let [ast (raql/compile heading-relmap {} raql)]
    (rh/transform ast)))


(defn sql
  ([expr]
   (sql/format (honey expr) {:quoted false}))
  ([expr params]
   (sql/format (honey expr) {:params params})))


(defn raql!
  "Executes a RAQL expression"
  ([expr]
   (jdbc/execute! chinook/ds (sql expr)
                  {:builder-fn builder}))
  ([expr params]
   (jdbc/execute! chinook/ds (sql expr params)
                  {:builder-fn builder})))


(defn sql!
  "Executes a SQL expression"
  ([expr]
   (jdbc/execute! chinook/ds expr
                  {:builder-fn builder})))


(defn honey!
  "Executes a HoneySQL expression"
  ([expr]
   (-> expr (sql/format) (sql!))))


(sql/format
  {:select [:artist-id [[:raw :to]]]
   :from :artist}
  {:quoted false})


(comment
  (sql '[restrict
         [relation :Artist]
         [= :Artist/ArtistId 7]])

  (raql!
    '[restrict
      [relation :Artist]
      [= :Artist/ArtistId ?id]]
    {:id 7}))


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

    ;; restrict in
    (raql! '[->
             [relation :Artist]
             [restrict [in :Artist/ArtistId [5 6 7]]]])

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\")
             where \"Artist/ArtistId\" in (5, 6, 7)"])

    (raql! '[->
             [relation :Album]
             [restrict [in :Album/ArtistId
                        [project
                         [restrict
                          [relation :Artist]
                          [in :Artist/ArtistId [5 6]]]
                         [:Artist/ArtistId]]]]])

    (sql!   ["select
               AlbumId \"Album/AlbumId\",
               Title \"Album/Title\",
               ArtistId \"Album/ArtistId\"

              from Album
              where ArtistId in
              (select \"Artist/ArtistId\"
               from
                 (select *
                  from
                  (select
                     \"ArtistId\" as \"Artist/ArtistId\",
                     \"Name\" as \"Artist/Name\"
                   from \"Artist\")
                  where \"Artist/ArtistId\" in (5, 6)))"])

    ;; restrict with params
    (raql! '[->
             [relation :Artist]
             [restrict [= :Artist/ArtistId ?id]]]
           {:id 5})

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\")
             where \"Artist/ArtistId\" = ?"
             5])


    (raql! '[->
             [relation :Artist]
             [restrict [in :Artist/ArtistId ?ids]]]
           {:ids [5 6]})

    (sql!   ["select *
             from
             (select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\"
              from \"Artist\")
             where \"Artist/ArtistId\" in (?, ?)"
             5 6])

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
             order by \"Artist/Name\" desc, \"Album/Title\""])

    ;; constant extensions
    (raql! '[->
             [relation :Artist]
             [extend {:Artist/Rating [:string "Awesome"]}]])

    (sql!   ["select
                \"ArtistId\" as \"Artist/ArtistId\",
                \"Name\" as \"Artist/Name\",
                'Awesome' as \"Artist/Rating\"
              from \"Artist\""])))


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


(defn table-name
  [relvar-name]
  (-> relvar-name (name) (str/replace #"-" "_")))


(defn column-name
  [_relvar-name attr-name]
  (-> attr-name (name) (str/replace #"-" "_")))


(deftest transform-with-table-column-names
  (is (= {:from [[[:raw "\"artist\""] :__relation]]
          :select [[[:raw "\"artist_id\""] [[:raw "\"artist/artist-id\""]]]
                   [[:raw "\"name\""] [[:raw "\"artist/name\""]]]]}
         (let [raql '[relation :artist]
               ast (raql/compile kebab-heading-relmap {} raql)]

           (as->
             ast $
             (rh/transform $ {:column-name-fn column-name})))))

  (is (= {:from [[[:raw "\"invoice_line\""] :__relation]]
          :select [[[:raw "\"invoice_line_id\""] [[:raw "\"invoice-line/invoice-line-id\""]]]
                   [[:raw "\"invoice_id\""] [[:raw "\"invoice-line/invoice-id\""]]]
                   [[:raw "\"track_id\""] [[:raw "\"invoice-line/track-id\""]]]
                   [[:raw "\"unit_price\""] [[:raw "\"invoice-line/unit-price\""]]]
                   [[:raw "\"quantity\""] [[:raw "\"invoice-line/quantity\""]]]]}

         (let [raql '[relation :invoice-line]
               ast (raql/compile kebab-heading-relmap {} raql)]

           (as->
             ast $
             (rh/transform $ {:table-name-fn table-name
                              :column-name-fn column-name}))))))

