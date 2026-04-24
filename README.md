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

## Data Layers

The pipeline is intentionally split by grain:

- `raw`: 1-to-1 snapshots of source payloads as JSONL files in MinIO or local disk
- `gold`: small source reference tables that should stay close to the upstream API, such as CBS measure, period, and region codes
- `bronze`: append-only Iceberg ingest from raw source records
- `silver`: normalized domain tables and source-specific transforms
- `landing`: Postgres tables used as the dbt input layer
- `marts`: dbt tables and views that combine BAG, CBS, and bridge data into analysis-ready outputs

The main rule is:

- raw is for replay and reproducibility
- gold is for source lookups and reference codes
- silver is for normalized data products
- landing is for SQL consumption
- marts are for cross-source analysis

The local validation loop is:

- `task test-unit` for fast code-level checks
- `task test-raw` for the raw landing-pad smoke test against MinIO
- `task test-integration` for the Bronze -> Silver -> landing roundtrip against the local stack
- `task cbs-flow` for the CBS-only raw -> Bronze -> Silver -> landing -> dbt flow
- `task load-all` for the full CBS + BAG data load
- `task load-smoke` for the full CBS + BAG data load with small limits
- `task check` for the standard local quality gate

## Quick Start

1. `task setup`
   Installs dependencies, starts the local stack, and initializes the MinIO bucket.
2. `task test-raw`
   Verifies the raw landing-pad in MinIO.
3. `task cbs-flow`
   Runs the full local flow from raw through Bronze, Silver, landing, and dbt.
4. `task load-all`
   Runs the full CBS + BAG data load, including raw archiving, Bronze, Silver, landing, the geo-bridge seed, and dbt.
5. `task load-smoke`
   Runs the same full flow with small limits.
6. `task reset-data`
   Stops the stack, removes volumes, and clears generated local data.

## Project Structure

- `src/fip`: application code
- `src/fip/ingestion`: source adapters and Bronze ingestion flow
- `src/fip/lakehouse/bronze`: Bronze Iceberg writer, factory, and sink protocols
- `src/fip/lakehouse/silver`: shared Silver infra in `core`, plus source-specific transforms and sinks
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
   The Silver package now splits shared sink mechanics into `silver/core` and
   source-specific transforms into `silver/cbs/...` and `silver/pdok_bag/...`.
5. BAG raw, Bronze, Silver, and landing slices are present for `verblijfsobject`, `pand`, and `adres`.
6. The BAG geo bridge now uses the `adres -> GMxxxx` MVP mapping, with Locatieserver only as fallback.
7. The Postgres landing full refresh and the `inspect-landing` CLI path are working.
8. The next implementation steps are raw-reuse for ingest, stronger BAG end-to-end validation, and more marts on top of the current reference dims.

## Current Data Flow

The local pipeline currently looks like this:

1. `archive-cbs-raw`
   Persist CBS source payloads as raw JSONL snapshots.
2. `ingest-cbs`
   Bronze ingest from raw CBS snapshots into Iceberg through Lakekeeper.
3. `inspect-bronze`
   DuckDB validation against the Bronze Iceberg tables.
4. `build-gold-measure-codes` / `build-gold-period-codes` / `build-gold-region-codes`
   Load CBS reference tables directly from raw snapshots into Postgres landing tables.
5. `build-cbs-silver-observations`
   Read Bronze rows, flatten them into Silver observations, and full-refresh the Silver Iceberg table.
6. `inspect-cbs-silver`
   DuckDB validation against the Silver Iceberg table.
7. `archive-bag-raw`
   Persist BAG source payloads as raw JSONL snapshots.
8. `ingest-bag`
   Bronze ingest from raw BAG snapshots into Iceberg through Lakekeeper.
9. `build-bag-silver-verblijfsobject`
   Build the first BAG Silver slice from `bag_verblijfsobject`.
10. `build-bag-silver-pand`
   Build the first BAG `pand` Silver slice from `bag_pand`.
11. `build-bag-silver-adressen`
   Build the BAG `adres` Silver slice, including the `GMxxxx` municipality key.
12. `build-bag-landing-verblijfsobject`
   Read BAG `verblijfsobject` Silver rows and full-refresh the Postgres landing table.
13. `build-bag-landing-pand`
   Read BAG `pand` Silver rows and full-refresh the Postgres landing table.
14. `build-bag-landing-adressen`
   Read BAG `adres` Silver rows and full-refresh the Postgres landing table.
15. `build-landing-observations`
   Read Silver rows and full-refresh the Postgres landing table.
16. `inspect-bag-landing-verblijfsobject` / `inspect-bag-landing-pand`
    Postgres readback of the BAG landing tables.
17. `inspect-landing`
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
