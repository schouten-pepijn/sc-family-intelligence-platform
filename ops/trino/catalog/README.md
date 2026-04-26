This directory is intentionally empty in the simplified local stack.

Current architecture:

- Python writes data to RustFS through its S3-compatible API
- DuckDB is the first local read/validation engine
- Trino runs as the SQL service layer

We do not keep an active Trino Iceberg catalog file here right now. Polaris is
the active REST catalog for Python and DuckDB workflows; if Trino-backed
Iceberg access is reintroduced, it should use the same Polaris catalog.

If we later reintroduce Trino-backed Iceberg access, we should do it through a
supported catalog type and document that choice in `docs/adr/`.
