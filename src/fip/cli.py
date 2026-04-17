import typer

from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.service import ingest_source_to_sink
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


def main() -> None:
    app()
