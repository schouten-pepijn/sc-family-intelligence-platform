from __future__ import annotations

import json

import typer

from fip.cli import app
from fip.commands._helpers import (
    iter_sampled_records,
)
from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.readback.duckdb import (
    attach_iceberg_catalog,
    count_rows,
    load_extensions,
    sample_rows,
)
from fip.readback.duckdb import connect as connect_duckdb
from fip.settings import get_settings


@app.command("inspect-cbs-raw")
def inspect_cbs_raw(
    table_id: str = typer.Option("83625NED", help="CBS table id to inspect."),
    run_id: str = typer.Option("debug-raw", help="Run id for the inspection session."),
    limit: int = typer.Option(5, help="Number of records to print per entity."),
    entity: str | None = typer.Option(
        None,
        help="Optional entity filter: Observations, MeasureCodes, PeriodenCodes, or RegioSCodes.",
    ),
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)
    for record in iter_sampled_records(source=source, entity=entity, limit=limit):
        typer.echo(record.entity_name)
        typer.echo(f"natural_key={record.natural_key}")
        typer.echo(json.dumps(record.payload, indent=2, ensure_ascii=False))
        typer.echo("")


@app.command("inspect-bronze")
def inspect_bronze(
    table_name: str = typer.Option(..., "--table", help="Bronze table name to inspect."),
    namespace: str | None = typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured bronze namespace.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    con = connect_duckdb()
    try:
        load_extensions(con)
        attach_iceberg_catalog(con)
        row_count = count_rows(con, table_name=table_name, namespace=namespace)
        rows = sample_rows(con, table_name=table_name, namespace=namespace, limit=limit)
    finally:
        con.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))


@app.command("inspect-cbs-silver")
def inspect_cbs_silver(
    table_name: str = typer.Option(
        "cbs_observations_flat_83625ned",
        "--table",
        help="Silver table name to inspect.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    silver_namespace = namespace or get_settings().silver_namespace
    con = connect_duckdb()
    try:
        load_extensions(con)
        attach_iceberg_catalog(con)
        row_count = count_rows(con, table_name=table_name, namespace=silver_namespace)
        rows = sample_rows(con, table_name=table_name, namespace=silver_namespace, limit=limit)
    finally:
        con.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))


@app.command("inspect-landing")
def inspect_landing(
    table_name: str = typer.Option(
        "cbs_observations_83625ned",
        "--table",
        help="Landing table name to inspect.",
    ),
    schema: str | None = typer.Option(
        None,
        help="Postgres schema to inspect. Defaults to configured landing schema.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    conn = connect_postgres()
    try:
        row_count = count_gold_rows(conn, table_name=table_name, schema=schema)
        rows = sample_gold_rows(conn, table_name=table_name, schema=schema, limit=limit)
    finally:
        conn.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))
