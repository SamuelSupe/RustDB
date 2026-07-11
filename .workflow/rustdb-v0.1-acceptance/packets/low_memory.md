# Packet: low_memory

Objective: orchestrate reproducible 64/128 MiB spill and performance suites.

Ownership: `benchmarks/suites/**` and `benchmarks/run_*.sh`. Do not edit TPC-H
generation, engine code, or CI files.

Verification: dry-run argument validation, execute a small dataset forward
test, validate JSON reports and spill cleanup assertions.
