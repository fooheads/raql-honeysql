# raql-honeysql

Translates compiled [RAQL](https://github.com/fooheads/raql) ASTs into HoneySQL
maps. SQL formatting and execution remain the caller's responsibility.

## Relational division

`divide` implements classical binary division using `SELECT DISTINCT` and
double `NOT EXISTS`, built entirely as HoneySQL structures. Both operands can
be base or derived relations, including parameterized restrictions and paged
queries. There is no division-specific database option; use the existing
`:db-type` and matching HoneySQL dialect described below.

```clojure
'[divide
  [relation :ItemTags]
  [rename [relation :RequiredTags] [[:RequiredTags/tag :ItemTags/tag]]]]

;; Equivalent threaded form:
'[-> [relation :ItemTags]
     [divide
      [rename [relation :RequiredTags] [[:RequiredTags/tag :ItemTags/tag]]]]]
```

For `ItemTags(item-id, tag)` containing `(1, A), (1, B), (2, A), (3, A),
(3, B), (3, C)` and `RequiredTags(tag)` containing `(A), (B)`, the result is
distinct item IDs 1 and 3. Extra values are allowed and duplicates do not
change the answer. All divisor attributes must exist in the dividend with
the same RAQL type; match the complete attribute keywords, using `rename`
where necessary. Multi-column requirements match complete tuples.

**An empty divisor returns only the distinct candidate tuples present in the
dividend.** An empty dividend always returns nothing, even if both operands
are empty. Applications wanting empty tag selection to include untagged items
must handle that case outside binary division.

Matching uses ordinary SQL `=`, consistent with existing join predicates.
NULL does not match NULL: a requirement containing any NULL is unsatisfiable,
and a candidate with a NULL key component cannot match a witness when the
divisor is nonempty. An empty divisor still retains NULL candidate keys.
`DISTINCT` eliminates duplicate result tuples, including duplicate NULL keys.

SQL has no portable zero-column result shape. For zero-attribute relations,
including division where both headings are equal and projection onto `[]`,
the adapter uses a constant column `__raql_unit` with value 1. One row represents
the empty tuple, and no rows represents the empty relation. This column is not
part of the RAQL heading; extensions and nonempty projections select the real
attributes rather than exposing it as an additional attribute.

Pass restriction parameters to HoneySQL's `:params`, and also to `transform`
if either operand uses named pagination. Values remain bound parameters,
including when the dividend appears twice in the generated SQL. Alias names
are allocated per transformation to avoid correlation collisions in nested
division. As with other relational wrappers, add an outer `order-by` for final
presentation ordering; existing SQL Server subquery ordering restrictions
still apply.

Collection-valued `in` parameters are not relations, and no relation-valued
parameter API is added. Use an existing base or derived relation as the divisor.

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

The new division compiler support is currently in the sibling RAQL sources,
not the pinned published RAQL `0.9.10` dependency. Run joint-source tests with
`raql` checked out beside this repository:

```bash
# Existing SQLite tests and embedded HSQLDB 2.7.4
clojure -M:test:test/local
# Alternatively: clojure -X:test:test/local
```

The embedded suites execute division results, including duplicates, NULLs,
empty relations, zero-attribute headings, nested division, and pagination.
SQL generation is covered for PostgreSQL, SQL Server, and HSQLDB; formatting
coverage is not proof of execution on an external engine.

External execution tests use isolated, uniquely named tables and remove them
afterward. Select an engine and provide a JDBC URL using environment variables:

```bash
export RAQL_TEST_DB=postgresql # or mssql
export RAQL_TEST_JDBC_URL='jdbc:postgresql://localhost/raql_test'
clojure -X:test:test/local:test/integration
```

Supply authentication through the JDBC connection settings appropriate to your
test database; never commit credentials. SQL Server URLs use
`jdbc:sqlserver://...`. The integration alias reports an explicit error if the
engine or URL is missing instead of skipping execution.

The production dependency and library versions are unchanged. Publishing
division support will require a separate coordinated release: RAQL first,
then the adapter with that published dependency. After that update, release
validation should use the normal tests without `:test/local`.
