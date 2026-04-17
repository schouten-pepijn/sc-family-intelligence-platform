import typer

from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.service import ingest_source_to_sink
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    connect as connect_duckdb,
    count_rows,
    load_extensions,
    sample_rows,
)
from fip.settings import get_settings
from fip.sink.factory import IcebergSinkFactory

app = typer.Typer(help="Family Intelligence Platform CLI.")


@app.callback()
def main_callback() -> None:
    """Family Intelligence Platform command group."""
    return None


@app.command("ingest-cbs")
def ingest_cbs(
    table_id: str = typer.Option(..., help="CBS table id, for example 83625NED."),
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    target_namespace: str | None = typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
) -> None:
    if target_namespace is None:
        target_namespace = get_settings().bronze_namespace

    source = CBSODataSource(table_id=table_id, run_id=run_id)
    sink_factory = IcebergSinkFactory(namespace=target_namespace)

    written = ingest_source_to_sink(source, sink_factory)
    typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


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
        attach_lakekeeper_catalog(con)

        row_count = count_rows(con, table_name=table_name, namespace=namespace)
        rows = sample_rows(con, table_name=table_name, namespace=namespace, limit=limit)
    finally:
        con.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))


def main() -> None:
    app()
