# SQL differential suite

`differential.sh` runs every `cases/*.sql` template through RustDB and the
pinned DuckDB 1.4.3 container, canonicalizes the complete CSV results, and
requires matching checksums. The generated CSV fixtures provide
`__DATA__`, `__NULL_DATA__`, `__OUTER_DATA__`, and `__INNER_DATA__`
placeholders.

Both engines emit `__RUSTDB_NULL__` for SQL NULL during this gate, so NULL is
not conflated with a genuine empty string by CSV canonicalization.

The v0.3 cases cover scalar functions, microsecond Timestamp casts and
intervals, single-argument DISTINCT aggregates, one-level correlated scalar,
IN/NOT IN, EXISTS/NOT EXISTS subqueries, and their NULL/empty-set behavior.

Files ending in `-error.sql` must fail in both engines and provide a matching
`.rustdb-pattern`. Planner/binder failures must include an AST line and column.
Files ending in `-runtime-error.sql` instead require the structured
`execution error:` prefix because an execution-time cardinality violation has
no meaningful AST failure position. Runtime errors remain strict expected
outcomes, not ignored or allow-failure cases.

Run the suite through OrbStack:

```sh
tools/sql/differential.sh
```
