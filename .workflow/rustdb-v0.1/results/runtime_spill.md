# Runtime and spill result

Accepted:

- Engine-owned fixed-size compute runtime with a bounded output channel.
- Global and per-query hierarchical reservations, cancellation, metrics, and
  query-scoped spill cleanup.
- External LZ4 Arrow IPC sort runs, recursively repartitioned aggregate spill,
  and Grace hash-join spill with bounded skew fallback.
- Dropping a partially consumed result synchronously cancels its compute
  producer and cleans query-scoped spill files.

Verification:

- Unit tests cover reservation accounting, cancellation, IPC spill, recursive
  aggregate/join repartitioning, skew fallback, and oversized sort batches.
- Integration tests force sort, aggregate, inner/left join spill under a 256
  KiB budget and assert result correctness, peak reservation, consumer-drop
  cancellation, and empty spill directories.

Risk:

- Retained operator state is reserved, but transient third-party decoder/footer
  allocations are not all visible to the engine memory pool.
