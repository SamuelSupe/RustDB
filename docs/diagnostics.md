# RustDB diagnostics

`rustdb diagnostics` creates a small, versioned JSON snapshot for support and
offline troubleshooting. It performs the same read-only Native integrity check
as `rustdb native check`; it does not open the database for recovery, migration,
repair, or query execution.

```sh
rustdb diagnostics --database /srv/rustdb/analytics
rustdb diagnostics \
  --database /srv/rustdb/analytics \
  --output rustdb-diagnostics.json
```

Without `--output`, the document is written to stdout. A file output is
published by atomic replacement in the destination directory, synced, and
created with mode `0600` on Unix. Existing symlinks and non-regular output
targets are rejected. To preserve the command's read-only database contract,
the output must be outside the Native database directory.

## Schema and contents

The top-level `schema_version` is currently `1`. Consumers must reject unknown
versions rather than guessing their meaning. The report contains:

- RustDB version and target OS/architecture;
- generation time in UTC;
- a domain-separated SHA-256 fingerprint of the database path, never the path;
- Native check counts, format/catalog generation and deduplicated issue codes;
- database byte size and filesystem total/available bytes when probes succeed;
- non-sensitive memory, concurrency, scan, Spill, Native quota and S3 mode
  configuration summaries.

The report deliberately excludes database and file paths, file names, file
contents, SQL, query results, tokens, token digests, credentials, S3 region and
endpoint values, credential-provider contents, table names, and Spill/result
directories. Probe failures use stable codes without embedding operating-system
error text.

Treat the JSON as operational metadata: review it before sharing and transfer
it through the same trusted support channel used for logs. It is not a backup
and cannot be used to repair or restore a database.
