# Orchestration

1. Root freezes the verified engine as an immutable local baseline.
2. TPC-H packet owns pinned dataset generation and DuckDB checksum comparison.
3. Low-memory packet owns spill-suite orchestration and benchmark manifests.
4. CI/docs packet owns platform-neutral quality gates and operator guidance.
5. Root integrates scripts, runs a small forward test first, then SF1.
6. Root evaluates the SF10 resource gate from actual disk and runtime evidence.
7. A separate read-only audit reviews correctness claims before the final
   acceptance commit.

Integration policy: generated data and measurements remain ignored artifacts;
only scripts, manifests, tests, and documentation are committed. Workers own
disjoint paths and must preserve concurrent changes.
