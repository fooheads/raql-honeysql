(ns fooheads.raql-honeysql.pagination-integration-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer [deftest]]
    [fooheads.raql-honeysql.pagination-fixture :as fixture]))


(deftest external-pagination-test
  (let [database (System/getenv "RAQL_TEST_DB")
        jdbc-url (System/getenv "RAQL_TEST_JDBC_URL")]
    (when-not (#{"postgresql" "mssql"} database)
      (throw (ex-info "Set RAQL_TEST_DB to postgresql or mssql"
                      {:database database})))
    (when (str/blank? jdbc-url)
      (throw (ex-info "Set RAQL_TEST_JDBC_URL for the selected integration database"
                      {:database database})))
    (fixture/with-database (keyword database) jdbc-url fixture/assert-pages!)))
