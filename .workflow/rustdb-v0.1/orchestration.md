# Orchestration

1. Root owns crate scaffolding, public API, CLI, integration, documentation, and final verification.
2. Runtime packet owns configuration, metrics, memory reservations, cancellation, and spill primitives.
3. Datasource packet owns URI/object-store handling and CSV/Parquet readers.
4. SQL packet owns AST binding, logical plans, expressions, optimizer rules, and physical operators.
5. Root integrates packets behind the public `Engine`/`Session` API and resolves compile or semantic conflicts.
6. Verify formatting, clippy, unit/integration tests, CLI smoke tests, spill cleanup, and MinIO access through OrbStack.

Integration policy: agents must not edit files outside their ownership, must preserve concurrent changes, and must report incomplete or unsupported behavior explicitly.
