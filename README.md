# raql-honeysql

Translates compiled [RAQL](https://github.com/fooheads/raql) ASTs into HoneySQL
maps. SQL formatting and execution remain the caller's responsibility.

## Limit and offset

Given a RAQL `heading-relmap` describing your relations and attributes:

```clojure
(require '[fooheads.raql.core :as raql]
         '[fooheads.raql-honeysql.core :as rh]
         '[honey.sql :as sql])

(let [expr '[-> [relation :Artist]
                [order-by [:Artist/ArtistId]]
                [offset ?start]
                [limit ?page-size]]
      params {:start 20 :page-size 10}
      ast (raql/compile heading-relmap expr)
      honey (rh/transform ast {:db-type :mssql :params params})]
  (sql/format honey {:dialect :sqlserver :params params}))
```

Use integer counts directly when named parameters are not needed. Limit-only
and offset-only queries are supported, as is the existing `[limit 10 20]`
shorthand for offset 20 followed by limit 10.

Named pagination values are resolved from `transform`'s `:params` map and must
be nonnegative integers. Missing or invalid bindings throw an exception.
Restriction parameters, such as `[= :Artist/ArtistId ?id]`, still use HoneySQL's
`:params`: pass the same map to both functions. Counts resolved by the adapter
are still parameterized by HoneySQL, not interpolated into SQL.

### Database configuration

Set `:db-type` in your per-database transformation options. There is no database
auto-detection or global dialect change.

| `transform` `:db-type` | HoneySQL `:dialect` | Pagination SQL |
| --- | --- | --- |
| `:postgresql` (default) | `:ansi` | `LIMIT ? OFFSET ?` |
| `:mssql` | `:sqlserver` | `TOP(?)` for limit-only; `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` with offset |
| `:hsqldb` | `:ansi` | `FETCH FIRST ? ROWS ONLY` for limit-only; `OFFSET ? ROWS FETCH NEXT ? ROWS ONLY` with offset |

Offset-only queries omit the limit/fetch clause. PostgreSQL and HSQLDB use
`OFFSET ?`; SQL Server uses `OFFSET ? ROWS`. HoneySQL can use singular `ROW`
when a count is one. Parameter order follows the SQL: limit then offset for
PostgreSQL, offset then fetch for SQL Server/HSQLDB.

HoneySQL's `:sqlserver` dialect does **not** turn `:limit` into SQL Server
pagination. Select both the adapter database type and the matching formatting
dialect. Existing options `:db-schema`, `:table-name-fn`, and `:column-name-fn`
continue to work.

The target engines are PostgreSQL, SQL Server 2012+ for OFFSET/FETCH, and HSQLDB
2.x. HSQLDB does not need a PostgreSQL or MySQL compatibility mode.
Identifiers retain the adapter's existing double-quoted representation, so
SQL Server connections must have `QUOTED_IDENTIFIER ON`.

### Ordering, composition, and zero

Place `order-by` immediately before a pagination chain, after relational
operations such as projection or restriction. Ordering and pagination are
lowered into the same SQL block.

SQL Server requires explicit `order-by` for any offset-bearing query, including
offset zero or the combined limit syntax. Limit-only uses TOP and can omit
ordering, but the selected rows are then unspecified. Unsupported unpaginated
ordering inside SQL Server subqueries is rejected rather than silently dropped.

Adjacent pagination steps preserve their sequential meaning. Repeated offsets
add, repeated limits choose the smaller count, and offset after limit reduces
the remaining count. A new relational operation or ordering starts a new query
scope. For example, sorting after limiting sorts the already selected subset.

Limit zero, including a bound named parameter or an exhausted pagination
window, produces a heading-preserving empty query on every engine. The adapter
does not emit FETCH 0 or HSQLDB's native LIMIT 0, which can mean unlimited.

An outer relational wrapper does not inherit its subquery's result ordering.
Add an outer `order-by` when the final presentation order matters. A unique
ordering does not prevent pages from shifting under concurrent writes, and
large offsets can be expensive. Keyset pagination is not implemented.

## Development

The pagination compiler and translator must be tested together. The `:test`
alias uses the sibling `../raql` checkout rather than the published RAQL
artifact. With `raql` checked out beside this repository:

```bash
# RAQL
cd ../raql
clojure -X:test

# Adapter: sibling RAQL sources, existing SQLite tests, and embedded HSQLDB 2.7.4
cd ../raql-honeysql
clojure -M:test
# Alternatively: clojure -X:test
```

External execution tests use isolated, uniquely named tables and remove them
afterward. Select an engine and provide a JDBC URL using environment variables:

```bash
export RAQL_TEST_DB=postgresql # or mssql
export RAQL_TEST_JDBC_URL='jdbc:postgresql://localhost/raql_test'
clojure -X:test:test/integration
```

Supply authentication through the JDBC connection settings appropriate to your
test database; never commit credentials. SQL Server URLs use
`jdbc:sqlserver://...`. The integration alias reports an explicit error if the
engine or URL is missing instead of skipping execution.

The production RAQL artifact dependency must be updated after a RAQL release
containing the pagination changes. Until then, the `:test` alias supplies the
local dependency override for tests; other development commands need an
equivalent local dependency override. This repository does not publish the
sibling library automatically.
