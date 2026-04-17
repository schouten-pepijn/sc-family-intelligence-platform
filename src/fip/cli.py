import typer

from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.service import ingest_source_to_sink
from fip.sink.iceberg_sink import IcebergSink

app = typer.Typer(help="Family Intelligence Platform CLI.")


@app.callback()
def main_callback() -> None:
    """Family Intelligence Platform command group."""
    return None


@app.command("ingest-cbs")
def ingest_cbs(
    table_id: str = typer.Option(..., help="CBS table id, for example 83625NED."),
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    target_table: str = typer.Option(..., help="Target sink table identifier."),
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)
    sink = IcebergSink(table_ident=target_table)

    written = ingest_source_to_sink(source, sink)
    typer.echo(f"Wrote {written} records to {target_table}")


def main() -> None:
    app()
