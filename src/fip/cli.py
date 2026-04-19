import json
from pathlib import Path
from typing import Iterator

import typer

from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.gold.reference_codes import ReferenceCodeWriter
from fip.gold.service import write_silver_rows_to_gold_sink
from fip.gold.writer import GoldObservationWriter
from fip.ingestion.base import RawRecord
from fip.ingestion.cbs.adapter import CBSODataSource
from fip.ingestion.pdok_bag.adapter import PDOKBAGSource
from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.bronze.cbs_factory import CBSIcebergSinkFactory
from fip.lakehouse.silver.bag_pand_service import (
    write_bronze_rows_to_bag_pand_sink,
)
from fip.lakehouse.silver.bag_pand_sink import BAGPandSink
from fip.lakehouse.silver.bag_verblijfsobject_service import (
    write_bronze_rows_to_bag_verblijfsobject_sink,
)
from fip.lakehouse.silver.bag_verblijfsobject_sink import BAGVerblijfsobjectSink
from fip.lakehouse.silver.cbs_observations_service import (
    write_bronze_rows_to_cbs_observation_sink,
)
from fip.lakehouse.silver.cbs_observations_sink import CBSObservationSink
from fip.raw.writer import MinioRawSnapshotWriter, RawSnapshotWriter
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

    for record in iter_sampled_cbs_raw_records(source=source, entity=entity, limit=limit):
        typer.echo(record.entity_name)
        typer.echo(f"natural_key={record.natural_key}")
        typer.echo(json.dumps(record.payload, indent=2, ensure_ascii=False))
        typer.echo("")


def iter_sampled_cbs_raw_records(
    source: CBSODataSource,
    entity: str | None,
    limit: int,
) -> Iterator[RawRecord]:
    count = 0
    for record in source.iter_records():
        if entity is not None and not record.entity_name.endswith(f".{entity}"):
            continue
        yield record
        count += 1
        if count >= limit:
            break


def iter_sampled_bag_raw_records(
    source: PDOKBAGSource,
    entity: str | None,
    limit: int,
) -> Iterator[RawRecord]:
    count = 0
    for record in source.iter_records():
        if entity is not None and not record.entity_name.endswith(f".{entity}"):
            continue
        yield record
        count += 1
        if count >= limit:
            break


def _iter_cbs_records_for_entity(
    source: CBSODataSource,
    entity: str,
) -> Iterator[RawRecord]:
    for record in source.iter_records():
        if record.entity_name.endswith(f".{entity}"):
            yield record


@app.command("ingest-cbs")
def ingest_cbs(
    table_id: str = typer.Option(..., help="CBS table id, for example 83625NED."),
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    target_namespace: str | None = typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
    limit: int = typer.Option(
        1000,
        min=1,
        help="Maximum number of CBS records to ingest.",
    ),
) -> None:
    if target_namespace is None:
        target_namespace = get_settings().bronze_namespace

    source = CBSODataSource(table_id=table_id, run_id=run_id)
    sink_factory = CBSIcebergSinkFactory(namespace=target_namespace)

    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0
    for record in source.iter_records():
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(records)

    typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@app.command("ingest-bag")
def ingest_bag(
    run_id: str = typer.Option(..., help="Run identifier for this ingestion."),
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to ingest, for example verblijfsobject or pand.",
    ),
    target_namespace: str | None = typer.Option(
        None,
        help="Target Iceberg namespace.",
    ),
    limit: int = typer.Option(
        1000,
        min=1,
        help="Maximum number of BAG records to ingest.",
    ),
    progress_every: int = typer.Option(
        1000,
        min=1,
        help="Print progress every N records while reading BAG.",
    ),
) -> None:
    if target_namespace is None:
        target_namespace = get_settings().bronze_namespace

    source = PDOKBAGSource(run_id=run_id, collection=collection)
    sink_factory = BAGIcebergSinkFactory(namespace=target_namespace)

    grouped_records: dict[str, list[RawRecord]] = {}
    seen = 0
    for record in source.iter_records():
        grouped_records.setdefault(record.entity_name, []).append(record)
        seen += 1
        if seen % progress_every == 0:
            typer.echo(f"Read {seen} BAG records...")
        if seen >= limit:
            break

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(records)

    typer.echo(f"Wrote {written} records using sink namespace {target_namespace}")


@app.command("inspect-bag-raw")
def inspect_bag_raw(
    run_id: str = typer.Option("debug-raw", help="Run id for the inspection session."),
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to inspect, for example verblijfsobject or pand.",
    ),
    limit: int = typer.Option(5, help="Number of records to print."),
    entity: str | None = typer.Option(
        None,
        help="Optional entity filter, for example verblijfsobject or pand.",
    ),
) -> None:
    source = PDOKBAGSource(run_id=run_id, collection=collection)
    for record in iter_sampled_bag_raw_records(
        source=source,
        entity=entity,
        limit=limit,
    ):
        typer.echo(record.entity_name)
        typer.echo(f"natural_key={record.natural_key}")
        typer.echo(json.dumps(record.payload, indent=2, ensure_ascii=False))
        typer.echo("")


@app.command("archive-bag-raw")
def archive_bag_raw(
    run_id: str = typer.Option("debug-raw"),
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to archive, for example verblijfsobject or pand.",
    ),
    limit: int = typer.Option(1000, help="Maximum number of raw records to archive."),
    target: str = typer.Option("local", help="Raw storage target: local JSONL files or MinIO."),
    output_dir: Path = Path(".raw"),
) -> None:
    source = PDOKBAGSource(run_id=run_id, collection=collection)

    writer: RawSnapshotWriter | MinioRawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "minio":
        writer = MinioRawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 'minio'")

    grouped: dict[str, list[RawRecord]] = {}
    archived = 0
    for record in source.iter_records():
        grouped.setdefault(record.entity_name, []).append(record)
        archived += 1
        if archived >= limit:
            break

    written = 0
    for records in grouped.values():
        written += writer.write(records)

    typer.echo(f"Wrote {written} raw records")


@app.command("archive-cbs-raw")
def archive_cbs_raw(
    table_id: str = typer.Option("83625NED"),
    run_id: str = typer.Option("debug-raw"),
    target: str = typer.Option(
        "local",
        help="Raw storage target: local JSONL files or MinIO object storage.",
    ),
    output_dir: Path = Path(".raw"),
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)

    writer: RawSnapshotWriter | MinioRawSnapshotWriter
    if target == "local":
        writer = RawSnapshotWriter(base_dir=str(output_dir))
    elif target == "minio":
        writer = MinioRawSnapshotWriter()
    else:
        raise typer.BadParameter("target must be either 'local' or 'minio'")

    grouped: dict[str, list[RawRecord]] = {}
    for record in source.iter_records():
        grouped.setdefault(record.entity_name, []).append(record)

    written = 0
    for records in grouped.values():
        written += writer.write(records)

    typer.echo(f"Wrote {written} raw records")


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


@app.command("inspect-bag-bronze")
def inspect_bag_bronze(
    collection: str = typer.Option(
        "verblijfsobject",
        help="BAG collection to inspect, for example verblijfsobject or pand.",
    ),
    namespace: str | None = typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured bronze namespace.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_bronze(
        table_name=f"bag_{collection}",
        namespace=namespace,
        limit=limit,
    )


@app.command("build-cbs-silver-observations")
def build_cbs_silver_observations(
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
    sink = CBSObservationSink(
        table_ident=f"{silver_namespace}.cbs_observations_flat_83625ned",
    )

    written = write_bronze_rows_to_cbs_observation_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} Silver rows")


@app.command("build-bag-silver-verblijfsobject")
def build_bag_silver_verblijfsobject(
    table_name: str = typer.Option(
        "bag_verblijfsobject",
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
    sink = BAGVerblijfsobjectSink(
        table_ident=f"{silver_namespace}.bag_verblijfsobject_flat",
    )

    written = write_bronze_rows_to_bag_verblijfsobject_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG Silver rows")


@app.command("build-bag-silver-pand")
def build_bag_silver_pand(
    table_name: str = typer.Option(
        "bag_pand",
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
    sink = BAGPandSink(
        table_ident=f"{silver_namespace}.bag_pand_flat",
    )

    written = write_bronze_rows_to_bag_pand_sink(bronze_rows, sink)
    typer.echo(f"Wrote {written} BAG Silver rows")


@app.command("build-gold-measure-codes")
def build_gold_measure_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    _build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="MeasureCodes",
        table_name="cbs_measure_codes",
    )


@app.command("build-gold-period-codes")
def build_gold_period_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    _build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="PeriodenCodes",
        table_name="cbs_period_codes",
    )


@app.command("build-gold-region-codes")
def build_gold_region_codes(
    table_id: str = typer.Option("83625NED", help="CBS table id to ingest."),
    run_id: str = typer.Option("debug-raw", help="Run identifier for this export."),
) -> None:
    _build_gold_reference_codes(
        table_id=table_id,
        run_id=run_id,
        entity="RegioSCodes",
        table_name="cbs_region_codes",
    )


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


def _build_gold_reference_codes(
    table_id: str,
    run_id: str,
    entity: str,
    table_name: str,
) -> None:
    source = CBSODataSource(table_id=table_id, run_id=run_id)
    records = list(_iter_cbs_records_for_entity(source=source, entity=entity))
    sink = ReferenceCodeWriter(table_name=table_name, entity=entity)

    written = sink.write(records)
    typer.echo(f"Wrote {written} {entity} rows into {table_name}")


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
        attach_lakekeeper_catalog(con)

        row_count = count_rows(con, table_name=table_name, namespace=silver_namespace)
        rows = sample_rows(con, table_name=table_name, namespace=silver_namespace, limit=limit)
    finally:
        con.close()

    typer.echo(f"Row count: {row_count}")
    typer.echo(f"Sample rows ({len(rows)}):")
    for row in rows:
        typer.echo(str(row))


@app.command("inspect-bag-silver")
def inspect_bag_silver(
    namespace: str | None = typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_cbs_silver(
        table_name="bag_verblijfsobject_flat",
        namespace=namespace,
        limit=limit,
    )


@app.command("inspect-bag-silver-pand")
def inspect_bag_silver_pand(
    namespace: str | None = typer.Option(
        None,
        help="Iceberg namespace to inspect. Defaults to configured silver namespace.",
    ),
    limit: int = typer.Option(5, help="Number of sample rows to display."),
) -> None:
    inspect_cbs_silver(
        table_name="bag_pand_flat",
        namespace=namespace,
        limit=limit,
    )


@app.command("build-landing-observations")
def build_landing_observations(
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
    typer.echo(f"Wrote {written} landing rows")


@app.command("inspect-landing")
def inspect_landing(
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
