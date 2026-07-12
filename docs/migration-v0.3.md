# Migrating from v0.2 to v0.3

`v0.3.0-alpha.1` keeps the public `Engine`, `Session`, `QueryResult`, table
registration, refresh, configuration, and Arrow streaming APIs source
compatible with `v0.2.0-alpha.1`. Applications do not need a Rust API migration.

The release broadens the SQL surface used by TPC-H. Existing identifiers that
match newly supported built-in function names remain ordinary column names
unless they are used as a function call. Catalog entries, views, and query
plans are still session-local and in memory.

## SQL behavior changes

- Scalar string, NULL, numeric, and date/time functions documented in
  [compatibility.md](compatibility.md) are now bound through typed signatures.
- Timezone-free `TIMESTAMP` literals and strict UTF-8/Date/Timestamp casts use
  microseconds internally. Timezone-aware values are never converted
  implicitly.
- Numeric coercion follows the TPC-H/DuckDB surface: Decimal mixed with Float
  promotes to Float64, and `/` returns Float64 even for two Decimal operands.
  Other Decimal arithmetic remains exact.
- Single-argument `COUNT`, `SUM`, and `AVG` accept `DISTINCT`. `DISTINCT` on
  `MIN` or `MAX` has the same result as the ordinary aggregate.
- One-level correlated scalar, `IN`, `NOT IN`, `EXISTS`, and `NOT EXISTS`
  subqueries are decorrelated into set-at-a-time joins. A scalar subquery that
  produces more than one row still fails with a cardinality error.
- `NOT IN` follows SQL three-valued logic: an unmatched right-side NULL can
  make the result UNKNOWN, while an empty right side makes `NOT IN` true even
  for a NULL left operand.

Queries that depend on window functions, set operations, RIGHT/FULL joins,
LATERAL, recursive correlation, or a correlation without an equality key
remain unsupported and fail during planning.

## Operational behavior

All workers created for a query now belong to a query-scoped task group. The
first worker error or panic cancels sibling work, and query Spill cleanup waits
for the group to converge. `QueryResult::cancel()` remains a non-blocking
signal; dropping a result delegates convergence and cleanup to the background
reaper.
