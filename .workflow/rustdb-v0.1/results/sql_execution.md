# SQL, optimizer, and execution result

Accepted:

- DuckDB-dialect parsing, catalog/binder, logical plan, checked expression
  evaluation, and vectorized RecordBatch execution without DataFusion.
- SELECT/CTE/derived tables, filtering, grouping/HAVING, DISTINCT, sorting,
  limits, equi joins, scalar/IN/EXISTS subqueries, temp views, and explain.
- Safe constant folding, predicate/limit/column pushdown, Top-K fetch, and
  per-node inner-join build-side selection while preserving output order.
- TPC-H acceptance query shapes Q1, Q3, Q6, Q11, Q12, Q13, and Q14 plan and
  execute against deterministic smoke relations.

Verification:

- SQL tests cover three-valued semantics, Decimal/date behavior, errors,
  subquery cardinality, dynamic views, projection pruning, and join semantics.
- EXPLAIN ANALYZE smoke output contains scan/operator plans and global metrics.

Risk:

- The optimizer does not perform a global multi-table join-tree search or a
  parallel partial/final aggregate phase; it chooses the smaller build input at
  each independent inner-join node.
