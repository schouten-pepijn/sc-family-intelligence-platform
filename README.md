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

The near-term goal is to land raw source snapshots in MinIO, write Bronze and Silver Iceberg tables through Lakekeeper, validate them locally with DuckDB, and materialize Gold tables in Postgres.

## Project Structure

- `src/fip`: application code
- `tests`: unit, integration, and fixture-based tests
- `sql`: dbt models
- `ops`: infrastructure configuration
- `docs/adr`: architecture decisions
- `configs`: runtime and scoring configuration

## Near-Term Plan

1. Keep the local stack minimal: MinIO + Trino in Docker, Python + DuckDB locally.
2. Replace the placeholder sink with a real Python write path to MinIO-backed Bronze/Iceberg data.
3. Use DuckDB for local readback and validation before hardening the Trino side further.
