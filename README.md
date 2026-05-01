# Family Intelligence Platform

Personal data platform for comparing Dutch regions, municipalities, and later specific locations for family housing decisions.

## Current Phase

The repo is in the first end-to-end data-platform phase. The current direction is:

- Docker:
  - RustFS for S3-compatible object storage
  - Postgres for the landing layer, with PostGIS enabled
  - Polaris as the Iceberg REST catalog
- Local:
  - Python ingestion and write scripts
  - DuckDB for validation and ad-hoc analysis against Iceberg data

The near-term goal is now to keep the raw -> Bronze -> Silver -> landing path stable, reuse raw snapshots for ingest and replay, and continue expanding the first dbt-backed SQL layer on top of the landing table in Postgres.
The first municipality consumable marts now start with:

- `mart_municipality_overview` as the municipality comparison entrypoint
- `mart_municipality_housing_snapshot` as the first compact housing snapshot
- `mart_municipality_woz_snapshot` as the separate municipality WOZ snapshot

The housing snapshot is intentionally staged:

1. `83625NED` for the first compact snapshot
2. `85035NED` for housing stock and typology

`85036NED` is now modelled separately as a municipality WOZ snapshot because
it has municipality and province coverage from 2019 onward, which maps cleanly
to `GMxxxx`.

`85792NED` is not part of the municipality snapshot. It only covers the
Netherlands, provinces, and the 4 largest municipalities, so it belongs in a
separate price-index mart instead of a `GMxxxx`-wide snapshot.

The BAG-to-municipality bridge now uses the BAG GeoPackage v2 spatial flow:

- `bridge_bag_to_geo_region` delegates to the GeoPackage v2 spatial bridge
- `bridge_bag_to_geo_region_legacy_code` preserves the old GM-code path for
  parity checks and rollback
- the old BAG API / GM-code path is retained in SQL for comparison and rollback,
  but it is no longer part of the default smoke task

## Data Layers

The pipeline is intentionally split by grain:

- `raw`: 1-to-1 snapshots of source payloads as JSONL files in RustFS or local disk
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
- `task test-raw` for the raw landing-pad smoke test against RustFS
- `task test-integration` for the Bronze -> Silver -> landing roundtrip against the local stack
- `task cbs-flow` for the CBS-only raw -> Bronze -> Silver -> landing -> dbt flow, isolated with `run_id=smoke-flow`
- `task load-woz` for the CBS 85036NED flow with its own raw -> Bronze -> Silver -> landing -> dbt path, isolated with `run_id=woz-load`, including the `EigendomCodes` codelist
- `task load-smoke` for the current CBS + BAG GeoPackage v2 data load with small limits, isolated with `run_id=smoke-load`
- `task check` for the standard local quality gate

## Quick Start

1. `task setup`
   Installs dependencies, starts the local stack, initializes the RustFS bucket, and initializes the Polaris catalog.
2. `task test-raw`
   Verifies the raw landing-pad in RustFS.
3. `task cbs-flow`
   Runs the CBS-only flow from raw through Bronze, Silver, landing, and dbt with `run_id=smoke-flow`.
4. `task load-woz`
   Runs the CBS 85036NED flow from raw through Bronze, Silver, landing, and dbt with `run_id=woz-load`, including the `EigendomCodes` codelist.
5. `task load-smoke`
   Runs the current CBS + BAG GeoPackage v2 flow with small limits and `run_id=smoke-load`.
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

1. Keep the local stack focused: RustFS + Postgres + Polaris in Docker, Python + DuckDB locally.
2. Bronze Iceberg writes through Polaris are working, and Bronze is append-only.
3. DuckDB readback and the `inspect-bronze` CLI path are working.
4. CBS Silver full refresh, DuckDB readback, and the `inspect-cbs-silver` CLI path are working.
   The Silver package now splits shared sink mechanics into `silver/core` and
   source-specific transforms into `silver/cbs/...` and `silver/pdok_bag/...`.
5. BAG GeoPackage raw, Bronze, Silver, and landing slices are present for
   `verblijfsobject`, `pand`, `woonplaats`, `ligplaats`, and `standplaats`.
6. The BAG raw archive path now flushes in chunks instead of buffering a whole collection in memory.
7. The BAG geo bridge now uses the GeoPackage v2 spatial municipality bridge by
   default; the GM-code path remains available as
   `bridge_bag_to_geo_region_legacy_code` for comparison and rollback.
8. The Postgres landing full refresh and the `inspect-landing` CLI path are working.
9. Raw-reuse for ingest is in place; each full-chain task keeps a single `run_id` end-to-end. The next implementation steps are stronger BAG end-to-end validation, more marts on top of the current reference dims, and possibly COPY-based Postgres landing writes if landing becomes the bottleneck.

## Current Data Flow

The local pipeline currently looks like this:

1. `archive-cbs-raw`
   Persist CBS source payloads as raw JSONL snapshots.
2. `ingest-cbs`
   Bronze ingest from raw CBS snapshots into Iceberg through Polaris.
3. `inspect-bronze`
   DuckDB validation against the Bronze Iceberg tables.
4. CBS reference code builders
   Load CBS measure, period, and region reference tables directly from raw snapshots into Postgres landing tables.
5. `build-cbs-silver-observations`
   Read Bronze rows, flatten them into Silver observations, and full-refresh the Silver Iceberg table.
6. `inspect-cbs-silver`
   DuckDB validation against the Silver Iceberg table.
7. `archive-bag-gpkg`
   Persist BAG GeoPackage layer rows as raw JSONL snapshots.
8. `ingest-bag-gpkg`
   Bronze ingest from raw BAG GeoPackage snapshots into Iceberg through Polaris.
9. `build-bag-gpkg-silver-*`
   Build normalized BAG GeoPackage Silver slices.
10. `build-bag-gpkg-landing-*`
   Read BAG GeoPackage Silver rows and full-refresh the Postgres landing tables.
11. `build-landing-observations`
   Read Silver rows and full-refresh the Postgres landing table.
12. `inspect-bag-landing-verblijfsobject` / `inspect-bag-landing-pand`
    Postgres readback of the BAG landing tables.
13. `inspect-landing`
   Postgres readback of the landing table.

Current write semantics:

- Bronze: append-only
- Silver: full refresh
- landing: full refresh

## Local Infra

The local infrastructure now consists of:

- `rustfs`: S3-compatible object storage for raw data and Iceberg table files
- `rustfs-init`: creates the configured bucket on demand during `task infra-init`
- `postgres`: landing store for dbt input tables, with PostGIS enabled
- `polaris`: serves the Iceberg REST catalog and management API
- `polaris-setup`: creates the initial RustFS-backed Polaris catalog during `task infra-init`

Default host endpoints:

- RustFS S3 API: `http://localhost:9000`
- RustFS Console: `http://localhost:9001`
- Postgres: `localhost:55432`
- Polaris Iceberg REST API: `http://localhost:8181/api/catalog`
- Polaris management API: `http://localhost:8181/api/management`
- Polaris health API: `http://localhost:8182/q/health`

Bring the stack up with:

```bash
docker compose up -d
```

The bootstrap sequence is:

1. `task infra-up` starts `rustfs`, `postgres`, and `polaris`.
2. `task infra-init` runs one-off init jobs for `rustfs-init` and `polaris-setup`.
3. The landing `postgres` image includes PostGIS, so the spatial dbt task can run against the same database as the landing models.
