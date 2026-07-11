# SF10 low-memory result

Status: clean-commit acceptance completed.

The accepted artifact is
`benchmarks/results/low-memory/20260711-alpha2-clean-r2/manifest.json`. It was
created by native release build
`a9688d65d792d683fa899491b2b8e4118cb8df0b` against the SF10 manifest digest
`016ecef9f79a6dde2cd1f2b88ad12aac8ebfa283f6d357f174bcb51c48c98ade`.

Final results:

| Case | Limit | Rows | Peak bytes | Spill bytes | Reported spill units | p50 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Sort | 64 MiB | 15,000,000 | 42,471,943 | 1,139,983,840 | 68 | 14,010.499 |
| Sort | 128 MiB | 15,000,000 | 84,155,783 | 736,513,360 | 34 | 9,435.195 |
| Aggregate | 64 MiB | 999,982 | 38,271,121 | 200,878,592 | 32 | 20,975.964 |
| Aggregate | 128 MiB | 999,982 | 72,733,585 | 181,443,904 | 32 | 22,335.486 |
| Inner Join | 64 MiB | 1 | 67,049,050 | 369,454,848 | 64 | 16,671.058 |
| Inner Join | 128 MiB | 1 | 120,736,026 | 369,454,848 | 64 | 14,842.823 |
| Left Join | 64 MiB | 1 | 67,046,961 | 216,636,224 | 64 | 3,869.440 |
| Left Join | 128 MiB | 1 | 134,122,865 | 216,636,224 | 64 | 3,838.247 |

All eight measured runs matched pinned DuckDB 1.4.3, stayed within their
configured reservation limit, produced non-zero Spill, reported cleanup, and
left both the Spill root and query directories empty. Reported spill units are
the engine metric count; they are not asserted to equal unique logical
partitions.

Checksums are stable across the two memory limits:

- Aggregate: `d597b8f2ce31ca231687e56ed345cb92a2dd366ee352bdab0385f62b84f7f854`
- Inner Join: `1ac51e10c9327361ca02dd90fcde096dacedc4e3ffe6f1d5029dd175a273f4c0`
- Left Join: `b8cf836ae92efe2b126b20fe5c4455b5541e5247f34a20549d57a0e0351261b7`
- Sort: `8190c49c5e8ae3528df0907474bbae2d8975da859f762bbb2347ac12fdb0ac84`

Earlier low-memory output directories are retained only as ignored diagnostic
history and are superseded by this exact-build artifact.
