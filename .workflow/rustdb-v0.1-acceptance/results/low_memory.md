# SF10 low-memory result

Status: superseded diagnostic; clean-commit acceptance rerun pending.

The strict suite was run from a clean output directory with checksum validation
enabled. Candidate artifact:
`benchmarks/results/low-memory/20260710T225721Z/manifest.json`.

Final results:

| Case | Limit | Rows | Peak bytes | Spill bytes | Reported spill units | Cleanup |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Sort | 64 MiB | 15,000,000 | 67,096,896 | 421,182,364 | 34 | yes |
| Sort | 128 MiB | 15,000,000 | 134,193,792 | 419,653,882 | 17 | yes |
| Aggregate | 64 MiB | 999,982 | 67,108,800 | 133,519,168 | 992 | yes |
| Aggregate | 128 MiB | 999,982 | 134,217,600 | 91,544,064 | 320 | yes |
| Inner Join | 64 MiB | 1 | 67,076,032 | 383,545,856 | 64 | yes |
| Inner Join | 128 MiB | 1 | 120,237,952 | 383,545,856 | 64 | yes |
| Left Join | 64 MiB | 1 | 67,076,064 | 219,765,312 | 64 | yes |
| Left Join | 128 MiB | 1 | 134,151,968 | 219,765,312 | 64 | yes |

All four query checksums matched DuckDB. The candidate manifest has eight runs and
`correctness.verified=true`; every peak is within its configured limit, every
run spilled, and the final Spill root contains no `query-*` directory.

Defects found and fixed before accepting the run:

- Join originally created one IPC file for every input-batch/partition pair,
  reaching more than 625,000 files. Incremental partition writers now coalesce
  batches and rotate at a bounded uncompressed size.
- Array object overhead from fragmented IPC batches was counted as persistent
  build data, causing 64/128 MiB runs to write 123 GB/50 GB recursively. Build
  estimation now uses logical Arrow buffers plus explicit concat, hash-table,
  reader-window, and metadata allowances, and compacts batches hierarchically.
- Join benchmark inputs use derived projections so only required keys/payloads
  enter physical Join and Spill state.
- Spill cleanup now verifies removal after syncing the parent directory.

This run predates the implementation commit and later memory-accounting fixes,
so it is retained only as diagnostic history. A new run from the exact clean
implementation commit is required before acceptance is complete.
