# Family Intelligence Platform

Personal data platform for comparing Dutch regions, municipalities, and later specific locations for family housing decisions.

## Current Phase

The repo is in the foundation stage. The current goal is to set up a clean Python project structure before adding infrastructure, ingestion adapters, and dbt models.

## Project Structure

- `src/fip`: application code
- `tests`: unit, integration, and fixture-based tests
- `sql`: dbt models
- `ops`: infrastructure configuration
- `docs/adr`: architecture decisions
- `configs`: runtime and scoring configuration

## Near-Term Plan

1. Finalize Python project configuration and developer tooling.
2. Add local infrastructure definitions for MinIO, Polaris, and Trino.
3. Implement the first CBS ingestion adapter and fixture-driven tests.
