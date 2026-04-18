import typer

from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.gold.service import write_silver_rows_to_gold_sink
from fip.gold.writer import GoldObservationWriter
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.service import ingest_source_to_sink
from fip.lakehouse.bronze.factory import IcebergSinkFactory
from fip.lakehouse.silver.service import write_bronze_rows_to_silver_sink
from fip.lakehouse.silver.writer import SilverObservationSink
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    count_rows,
    load_extensions,
    sample_rows,
)
from fip.readback.duckdb import (
    connect as connect_duckdb,
)
from fip.settings import get_settings

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


@app.command("build-silver-observations")
def build_silver_observations(
    table_name: str = typer.Option(
        "cbs_observations_83625ned",
        "--table",
        help="Bronze table name to transform into Silver.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Bronze Iceberg namespace. Defaults to configured bronze namespace.",
    ),
) -> None:
    bronze_rows = _read_bronze_rows(table_name=table_name, namespace=namespace)
    silver_namespace = get_settings().silver_namespace
    sink = SilverObservationSink(
        table_ident=f"{silver_namespace}.cbs_observations_flat_83625ned",
    )

    written = write_bronze_rows_to_silver_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} Silver rows")


def _read_bronze_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()

    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)

        query = (
            "SELECT * "
            f"FROM lakekeeper_catalog.{namespace or get_settings().bronze_namespace}.{table_name}"
        )
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


def _read_silver_rows(
    table_name: str,
    namespace: str | None = None,
) -> list[dict[str, object]]:
    conn = connect_duckdb()

    try:
        load_extensions(conn)
        attach_lakekeeper_catalog(conn)

        query = (
            "SELECT * "
            f"FROM lakekeeper_catalog.{namespace or get_settings().silver_namespace}.{table_name}"
        )
        return conn.execute(query).to_arrow_table().to_pylist()
    finally:
        conn.close()


@app.command("inspect-silver")
def inspect_silver(
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
        attach_lakekeeper_catalog(con)

        row_count = count_rows(con, table_name=table_name, namespace=silver_namespace)
        rows = sample_rows(con, table_name=table_name, namespace=silver_namespace, limit=limit)
    finally:
        con.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))


@app.command("build-gold-observations")
def build_gold_observations(
    table_name: str = typer.Option(
        "cbs_observations_flat_83625ned",
        "--table",
        help="Silver table name to materialize into the Postgres landing layer.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Silver Iceberg namespace. Defaults to configured silver namespace.",
    ),
) -> None:
    silver_rows = _read_silver_rows(table_name=table_name, namespace=namespace)
    sink = GoldObservationWriter(table_name="cbs_observations")

    written = write_silver_rows_to_gold_sink(silver_rows, sink)
    typer.echo(f"Wrote {written} Gold rows")


@app.command("inspect-gold")
def inspect_gold(
    table_name: str = typer.Option(
        "cbs_observations",
        "--table",
        help="Gold table name to inspect.",
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


def main() -> None:
    app()
