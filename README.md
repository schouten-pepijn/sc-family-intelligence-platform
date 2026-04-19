# Family Intelligence Platform

Personal data platform for comparing Dutch regions, municipalities, and later specific locations for family housing decisions.

## Current Phase

The repo is in the first end-to-end data-platform phase. The current direction is:

- Docker:
  - MinIO for object storage
  - Postgres for Lakekeeper persistence and the landing layer
  - Lakekeeper as the Iceberg REST catalog
- Local:
  - Python ingestion and write scripts
  - DuckDB for validation and ad-hoc analysis against Iceberg data

The near-term goal is now to stabilize the working raw -> Bronze -> Silver -> landing path and continue expanding the first dbt-backed SQL layer on top of the landing table in Postgres.

The local validation loop is:

- `task test-unit` for fast code-level checks
- `task test-raw` for the raw landing-pad smoke test against MinIO
- `task test-integration` for the Bronze -> Silver -> landing roundtrip against the local stack
- `task test-flow` for the full raw -> Bronze -> Silver -> landing -> dbt flow
- `task check` for the standard local quality gate

## Quick Start

1. `task setup`
   Installs dependencies, starts the local stack, and initializes the MinIO bucket.
2. `task test-raw`
   Verifies the raw landing-pad in MinIO.
3. `task test-flow`
   Runs the full local flow from raw through Bronze, Silver, landing, and dbt.
4. `task reset-data`
   Stops the stack, removes volumes, and clears generated local data.

## Project Structure

- `src/fip`: application code
- `src/fip/ingestion`: source adapters and Bronze ingestion flow
- `src/fip/lakehouse/bronze`: Bronze Iceberg writer, factory, and sink protocols
- `src/fip/lakehouse/silver`: Silver transforms, orchestration, and writer
- `src/fip/gold`: Postgres landing writer, service, and readback helpers
- `src/fip/readback`: DuckDB validation and inspect helpers
- `tests`: unit, integration, and fixture-based tests
- `sql`: dbt models
- `ops`: infrastructure configuration
- `docs/adr`: architecture decisions
- `configs`: runtime and scoring configuration

## Near-Term Plan

1. Keep the local stack focused: MinIO + Postgres + Lakekeeper in Docker, Python + DuckDB locally.
2. Bronze Iceberg writes through Lakekeeper are working, and Bronze is append-only.
3. DuckDB readback and the `inspect-bronze` CLI path are working.
4. CBS Silver full refresh, DuckDB readback, and the `inspect-cbs-silver` CLI path are working.
5. The Postgres landing full refresh and the `inspect-landing` CLI path are working.
6. The next implementation step is to make the SQL/dbt layer concrete on top of the landing table.

## Current Data Flow

The local pipeline currently looks like this:

1. `ingest-cbs`
   Bronze ingest from the CBS API into Iceberg through Lakekeeper.
2. `inspect-bronze`
   DuckDB validation against the Bronze Iceberg tables.
3. `build-cbs-silver-observations`
   Read Bronze rows, flatten them into Silver observations, and full-refresh the Silver Iceberg table.
4. `inspect-cbs-silver`
   DuckDB validation against the Silver Iceberg table.
5. `build-landing-observations`
   Read Silver rows and full-refresh the Postgres landing table.
6. `inspect-landing`
   Postgres readback of the landing table.

Current write semantics:

- Bronze: append-only
- Silver: full refresh
- landing: full refresh

## Local Infra

The local infrastructure now consists of:

- `minio`: object storage for raw data and Iceberg table files
- `minio-init`: creates the configured bucket on startup
- `postgres`: metadata database for Lakekeeper and future landing store
- `lakekeeper-migrate`: runs catalog migrations once
- `lakekeeper`: serves the UI, management API, and Iceberg REST catalog
- `lakekeeper-bootstrap`: bootstraps the catalog in unsecured local mode
- `lakekeeper-create-warehouse`: creates the initial MinIO-backed warehouse

Default host endpoints:

- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Postgres: `localhost:55432`
- Lakekeeper UI and API: `http://localhost:8181`

Bring the stack up with:

```bash
docker compose up -d
```

The first startup sequence is:

1. `minio` and `postgres` come up.
2. `minio-init` creates the bucket.
3. `lakekeeper-migrate` initializes the catalog schema in Postgres.
4. `lakekeeper` starts serving on port `8181`.
5. `lakekeeper-bootstrap` accepts the local terms bootstrap.
6. `lakekeeper-create-warehouse` registers the first MinIO-backed warehouse.
