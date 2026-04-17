This directory is intentionally empty in the simplified local stack.

Current architecture:

- Python writes data to MinIO
- DuckDB is the first local read/validation engine
- Trino runs as the SQL service layer

We do not keep an active Iceberg catalog file here right now because the
previous Polaris-based REST catalog has been removed, and Trino does not
support a direct Iceberg Hadoop catalog configuration in the same way that
Python/DuckDB workflows do.

If we later reintroduce Trino-backed Iceberg access, we should do it through a
supported catalog type and document that choice in `docs/adr/`.
