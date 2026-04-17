# Family Intelligence Platform

Personal data platform for comparing Dutch regions, municipalities, and later specific locations for family housing decisions.

## Current Phase

The repo is in the first end-to-end data-platform phase. The current direction is:

- Docker:
  - MinIO for object storage
  - Postgres for Lakekeeper persistence and gold serving
  - Lakekeeper as the Iceberg REST catalog
- Local:
  - Python ingestion and write scripts
  - DuckDB for validation and ad-hoc analysis against Iceberg data

The near-term goal is to build the first Silver Iceberg table on top of the now-working Bronze write and DuckDB readback path, and only then materialize Gold tables in Postgres.

## Project Structure

- `src/fip`: application code
- `tests`: unit, integration, and fixture-based tests
- `sql`: dbt models
- `ops`: infrastructure configuration
- `docs/adr`: architecture decisions
- `configs`: runtime and scoring configuration

## Near-Term Plan

1. Keep the local stack focused: MinIO + Postgres + Lakekeeper in Docker, Python + DuckDB locally.
2. Bronze Iceberg writes through Lakekeeper are working.
3. DuckDB readback and the `inspect-bronze` CLI path are working.
4. The next implementation step is the first Silver transform.

## Local Infra

The local infrastructure now consists of:

- `minio`: object storage for raw data and Iceberg table files
- `minio-init`: creates the configured bucket on startup
- `postgres`: metadata database for Lakekeeper and future gold store
- `lakekeeper-migrate`: runs catalog migrations once
- `lakekeeper`: serves the UI, management API, and Iceberg REST catalog
- `lakekeeper-bootstrap`: bootstraps the catalog in unsecured local mode
- `lakekeeper-create-warehouse`: creates the initial MinIO-backed warehouse

Default host endpoints:

- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- Postgres: `localhost:5432`
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
