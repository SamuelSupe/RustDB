# TPC-H harness result

Status: completed.

Implementation:

- DuckDB is pinned to 1.4.3 in a checksum-verified amd64/arm64 container; the
  host does not need DuckDB.
- Dataset generation uses one thread, stable key ordering, zstd level 3, and
  122,880-row Parquet row groups.
- The seven-query loop disconnects child stdin so every entry is executed.
  Empty and header-only zero-row results canonicalize identically.
- TPC-H comma joins were rewritten as equivalent explicit keyed joins. The SQL
  smoke test rejects any `Join keys=0` plan in the acceptance set.

Engine fixes discovered by the strict reference comparison:

- `AVG(Decimal128)` now returns an untruncated `Float64`, including partial and
  Spill merge paths.
- Decimal comparisons that need more than precision 38 for a common physical
  type retain both scales and compare exactly.

Evidence:

- `tools/tpch/compare.sh 0.001`: all seven checksums matched.
- `tools/tpch/compare.sh 1`: all seven checksums matched.
- The final SF1 run was repeated on accepted implementation
  `a9688d65d792d683fa899491b2b8e4118cb8df0b`.
- SF1 canonical checksums:
  - Q1 `488b66ccd595fb84bc6ab9decdb1ee469d7398cd206222ded10cf5365f77a532`
  - Q3 `90f24d9edaedbb9912f9c0a09a65e527cb4e5812f8722a714dcba9503339e0c8`
  - Q6 `c9297f652a12b85067477fd755b124cf0346cccc6636b49355eab574c764be85`
  - Q11 `490dc3b6b7a77e90f16e2bd075cb2976a2309333396d29e06f5a7bb65c1b064e`
  - Q12 `b507de187b814ee216b44dc30341526dc487c3469c2ec1aa53a34593cf0f6c3c`
  - Q13 `2bb6bfbdfbc1f7cce31cce054575e60d46efe4eb4a32d7139093144210bc78f0`
  - Q14 `b7e09cca16a2680ec4cd24c08ddbce7c20021301a93d3065dccba9cda0e35a2e`
- SF1 contains 226,756,631 Parquet bytes; SF10 contains 2,355,369,249
  Parquet bytes. Both generated manifests passed full SHA-256 verification.

Generated data and result files remain ignored artifacts.
