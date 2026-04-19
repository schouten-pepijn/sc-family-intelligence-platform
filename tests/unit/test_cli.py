from datetime import datetime, timezone
from typing import cast

import pytest
from typer.testing import CliRunner

from fip import cli
from fip.ingestion.base import RawRecord

runner = CliRunner()


def test_ingest_cbs_command_invokes_service_and_prints_result(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            calls["table_id"] = table_id
            calls["run_id"] = run_id

    class FakeSinkFactory:
        def __init__(self, namespace: str) -> None:
            calls["target_namespace"] = namespace

    def fake_ingest_source_to_sink(source, sink_factory) -> int:
        calls["source"] = source
        calls["sink_factory"] = sink_factory
        return 7

    monkeypatch.setattr(cli, "CBSODataSource", FakeSource)
    monkeypatch.setattr(cli, "CBSIcebergSinkFactory", FakeSinkFactory)
    monkeypatch.setattr(cli, "ingest_source_to_sink", fake_ingest_source_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "ingest-cbs",
            "--table-id",
            "83625NED",
            "--run-id",
            "run-001",
            "--target-namespace",
            "bronze",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 7 records using sink namespace bronze\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "run-001"
    assert calls["target_namespace"] == "bronze"


def test_ingest_bag_command_invokes_service_and_prints_result(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, run_id: str) -> None:
            calls["run_id"] = run_id

    class FakeSinkFactory:
        def __init__(self, namespace: str) -> None:
            calls["target_namespace"] = namespace

    def fake_ingest_source_to_sink(source, sink_factory) -> int:
        calls["source"] = source
        calls["sink_factory"] = sink_factory
        return 5

    monkeypatch.setattr(cli, "PDOKBAGSource", FakeSource)
    monkeypatch.setattr(cli, "BAGIcebergSinkFactory", FakeSinkFactory)
    monkeypatch.setattr(cli, "ingest_source_to_sink", fake_ingest_source_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "ingest-bag",
            "--run-id",
            "run-002",
            "--target-namespace",
            "bronze",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 5 records using sink namespace bronze\n"
    assert calls["run_id"] == "run-002"
    assert calls["target_namespace"] == "bronze"


def test_inspect_cbs_raw_command_prints_filtered_payloads(monkeypatch) -> None:
    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            self.table_id = table_id
            self.run_id = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.Observations",
                natural_key="1",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Id": 1, "Measure": "M001534"},
                schema_version="v1",
            )
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="10",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Id": 10, "Title": "Measure A"},
                schema_version="v1",
            )

    monkeypatch.setattr(cli, "CBSODataSource", FakeSource)

    result = runner.invoke(
        cli.app,
        [
            "inspect-cbs-raw",
            "--entity",
            "MeasureCodes",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        '83625NED.MeasureCodes\nnatural_key=10\n{\n  "Id": 10,\n  "Title": "Measure A"\n}\n\n'
    )


def test_inspect_bag_raw_command_prints_filtered_payloads(monkeypatch) -> None:
    class FakeSource:
        def __init__(self, run_id: str) -> None:
            self.run_id = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="bag_pdok",
                entity_name="bag.verblijfsobject",
                natural_key="0003010000126809",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"identificatie": "0003010000126809", "postcode": "9901CP"},
                schema_version="v1",
            )

    monkeypatch.setattr(cli, "PDOKBAGSource", FakeSource)

    result = runner.invoke(
        cli.app,
        [
            "inspect-bag-raw",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        "bag.verblijfsobject\nnatural_key=0003010000126809\n{\n"
        '  "identificatie": "0003010000126809",\n  "postcode": "9901CP"\n}\n\n'
    )


@pytest.mark.parametrize(
    ("target", "expected_writer"),
    [
        ("local", "local"),
        ("minio", "minio"),
    ],
)
def test_archive_cbs_raw_command_selects_target_writer(
    monkeypatch, target: str, expected_writer: str
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            calls["table_id"] = table_id
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.MeasureCodes",
                natural_key="M001534",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Identifier": "M001534", "Title": "Measure A"},
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

    class FakeMinioWriter:
        def __init__(self) -> None:
            calls["writer"] = "minio"

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

    monkeypatch.setattr(cli, "CBSODataSource", FakeSource)
    monkeypatch.setattr(cli, "RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr(cli, "MinioRawSnapshotWriter", FakeMinioWriter)

    result = runner.invoke(
        cli.app,
        [
            "archive-cbs-raw",
            "--table-id",
            "83625NED",
            "--run-id",
            "debug-raw",
            "--target",
            target,
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 raw records\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "debug-raw"
    assert calls["writer"] == expected_writer
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert rows[0].entity_name == "83625NED.MeasureCodes"
    if target == "local":
        assert calls["base_dir"] == ".raw-smoke"


@pytest.mark.parametrize(
    ("target", "expected_writer"),
    [
        ("local", "local"),
        ("minio", "minio"),
    ],
)
def test_archive_bag_raw_command_selects_target_writer(
    monkeypatch, target: str, expected_writer: str
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, run_id: str) -> None:
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="bag_pdok",
                entity_name="bag.verblijfsobject",
                natural_key="0003010000126809",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"identificatie": "0003010000126809", "postcode": "9901CP"},
                schema_version="v1",
                http_status=200,
            )

    class FakeLocalWriter:
        def __init__(self, base_dir: str) -> None:
            calls["writer"] = "local"
            calls["base_dir"] = base_dir

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

    class FakeMinioWriter:
        def __init__(self) -> None:
            calls["writer"] = "minio"

        def write(self, records) -> int:
            rows = list(records)
            calls["rows"] = rows
            return len(rows)

    monkeypatch.setattr(cli, "PDOKBAGSource", FakeSource)
    monkeypatch.setattr(cli, "RawSnapshotWriter", FakeLocalWriter)
    monkeypatch.setattr(cli, "MinioRawSnapshotWriter", FakeMinioWriter)

    result = runner.invoke(
        cli.app,
        [
            "archive-bag-raw",
            "--run-id",
            "debug-raw",
            "--target",
            target,
            "--output-dir",
            ".raw-smoke",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 raw records\n"
    assert calls["run_id"] == "debug-raw"
    assert calls["writer"] == expected_writer
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert rows[0].entity_name == "bag.verblijfsobject"
    if target == "local":
        assert calls["base_dir"] == ".raw-smoke"


@pytest.mark.parametrize(
    ("command", "expected_entity", "expected_table_name", "expected_natural_key"),
    [
        (
            "build-gold-measure-codes",
            "MeasureCodes",
            "cbs_measure_codes",
            "M001534",
        ),
        (
            "build-gold-period-codes",
            "PeriodenCodes",
            "cbs_period_codes",
            "1995JJ00",
        ),
        (
            "build-gold-region-codes",
            "RegioSCodes",
            "cbs_region_codes",
            "NL01",
        ),
    ],
)
def test_build_gold_reference_commands_filter_records_and_write(
    monkeypatch,
    command: str,
    expected_entity: str,
    expected_table_name: str,
    expected_natural_key: str,
) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            calls["table_id"] = table_id
            calls["run_id"] = run_id

        def iter_records(self):
            yield RawRecord(
                source_name="cbs_statline",
                entity_name="83625NED.Observations",
                natural_key="1",
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Identifier": "IGNORED", "Title": "Ignored"},
                schema_version="v1",
            )
            yield RawRecord(
                source_name="cbs_statline",
                entity_name=f"83625NED.{expected_entity}",
                natural_key=expected_natural_key,
                retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
                run_id="debug-raw",
                payload={"Identifier": expected_natural_key, "Title": "Matched"},
                schema_version="v1",
            )

    class FakeWriter:
        def __init__(self, table_name: str, entity: str) -> None:
            calls["table_name"] = table_name
            calls["entity"] = entity
            self.rows: list[RawRecord] = []

        def write(self, rows):
            self.rows = list(rows)
            calls["rows"] = self.rows
            return len(self.rows)

    monkeypatch.setattr(cli, "CBSODataSource", FakeSource)
    monkeypatch.setattr(cli, "ReferenceCodeWriter", FakeWriter)

    result = runner.invoke(
        cli.app,
        [
            command,
            "--table-id",
            "83625NED",
            "--run-id",
            "debug-raw",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == f"Wrote 1 {expected_entity} rows into {expected_table_name}\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "debug-raw"
    assert calls["table_name"] == expected_table_name
    assert calls["entity"] == expected_entity
    rows = cast(list[RawRecord], calls["rows"])
    assert len(rows) == 1
    assert rows[0].entity_name == f"83625NED.{expected_entity}"
    assert rows[0].natural_key == expected_natural_key


def test_inspect_bronze_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_duckdb() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_load_extensions(con: object) -> None:
        calls["extensions_loaded"] = con

    def fake_attach_lakekeeper_catalog(con: object) -> None:
        calls["catalog_attached"] = con

    def fake_count_rows(con: object, table_name: str, namespace: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_namespace"] = namespace
        return 23095

    def fake_sample_rows(
        con: object,
        table_name: str,
        namespace: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_namespace"] = namespace
        calls["sample_limit"] = limit
        return [("row-1", "value-1"), ("row-2", "value-2")]

    monkeypatch.setattr(cli, "connect_duckdb", fake_connect_duckdb)
    monkeypatch.setattr(cli, "load_extensions", fake_load_extensions)
    monkeypatch.setattr(cli, "attach_lakekeeper_catalog", fake_attach_lakekeeper_catalog)
    monkeypatch.setattr(cli, "count_rows", fake_count_rows)
    monkeypatch.setattr(cli, "sample_rows", fake_sample_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-bronze",
            "--table",
            "cbs_observations_83625ned",
            "--limit",
            "2",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == (
        "Row count: 23095\nSample rows (2):\n('row-1', 'value-1')\n('row-2', 'value-2')\n"
    )
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_83625ned"
    assert calls["count_namespace"] is None
    assert calls["sample_table_name"] == "cbs_observations_83625ned"
    assert calls["sample_namespace"] is None
    assert calls["sample_limit"] == 2
    assert calls["closed"] is True


def test_build_silver_observations_command_reads_bronze_and_writes_silver(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeSilverSink:
        def __init__(self, table_ident: str) -> None:
            self.table_ident = table_ident
            calls["table_ident"] = table_ident

    def fake_read_bronze_rows(table_name: str, namespace: str | None) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "payload": (
                    '{"Id": 1, "Measure": "M001534", "Perioden": '
                    '"1995JJ00", "RegioS": "NL01", "StringValue": null, '
                    '"Value": 93750.0, "ValueAttribute": "None"}'
                ),
            }
        ]

    def fake_write_bronze_rows_to_silver_sink(
        bronze_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["bronze_rows"] = bronze_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr(cli, "_read_bronze_rows", fake_read_bronze_rows)
    monkeypatch.setattr(cli, "SilverObservationSink", FakeSilverSink)
    monkeypatch.setattr(
        cli, "write_bronze_rows_to_silver_sink", fake_write_bronze_rows_to_silver_sink
    )

    result = runner.invoke(
        cli.app,
        [
            "build-silver-observations",
            "--table",
            "cbs_observations_83625ned",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 Silver rows\n"
    assert calls["read_table_name"] == "cbs_observations_83625ned"
    assert calls["read_namespace"] is None
    assert calls["table_ident"] == "silver.cbs_observations_flat_83625ned"
    bronze_rows = cast(list[dict[str, object]], calls["bronze_rows"])
    assert len(bronze_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeSilverSink)
    assert sink.table_ident == "silver.cbs_observations_flat_83625ned"


def test_inspect_silver_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_duckdb() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_load_extensions(con: object) -> None:
        calls["extensions_loaded"] = con

    def fake_attach_lakekeeper_catalog(con: object) -> None:
        calls["catalog_attached"] = con

    def fake_count_rows(con: object, table_name: str, namespace: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_namespace"] = namespace
        return 42

    def fake_sample_rows(
        con: object,
        table_name: str,
        namespace: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_namespace"] = namespace
        calls["sample_limit"] = limit
        return [("silver-row-1", "value-1")]

    monkeypatch.setattr(cli, "connect_duckdb", fake_connect_duckdb)
    monkeypatch.setattr(cli, "load_extensions", fake_load_extensions)
    monkeypatch.setattr(cli, "attach_lakekeeper_catalog", fake_attach_lakekeeper_catalog)
    monkeypatch.setattr(cli, "count_rows", fake_count_rows)
    monkeypatch.setattr(cli, "sample_rows", fake_sample_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-silver",
            "--table",
            "cbs_observations_flat_83625ned",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Row count: 42\nSample rows (1):\n('silver-row-1', 'value-1')\n"
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["count_namespace"] == "silver"
    assert calls["sample_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["sample_namespace"] == "silver"
    assert calls["sample_limit"] == 1
    assert calls["closed"] is True


def test_build_gold_observations_command_reads_silver_and_writes_gold(
    monkeypatch,
) -> None:
    calls: dict[str, object] = {}

    class FakeGoldSink:
        def __init__(self, table_name: str) -> None:
            self.table_name = table_name
            calls["table_name"] = table_name

    def fake_read_silver_rows(table_name: str, namespace: str | None) -> list[dict[str, object]]:
        calls["read_table_name"] = table_name
        calls["read_namespace"] = namespace
        return [
            {
                "source_name": "cbs_statline",
                "natural_key": "1",
                "retrieved_at": "2026-04-18T09:00:00Z",
                "run_id": "run-001",
                "schema_version": "v1",
                "http_status": 200,
                "observation_id": 1,
                "measure_code": "M001534",
                "period_code": "1995JJ00",
                "region_code": "NL01",
                "numeric_value": 93750.0,
                "value_attribute": "None",
                "string_value": None,
            }
        ]

    def fake_write_silver_rows_to_gold_sink(
        silver_rows: list[dict[str, object]],
        sink: object,
    ) -> int:
        calls["silver_rows"] = silver_rows
        calls["sink"] = sink
        return 1

    monkeypatch.setattr(cli, "_read_silver_rows", fake_read_silver_rows)
    monkeypatch.setattr(cli, "GoldObservationWriter", FakeGoldSink)
    monkeypatch.setattr(cli, "write_silver_rows_to_gold_sink", fake_write_silver_rows_to_gold_sink)

    result = runner.invoke(
        cli.app,
        [
            "build-gold-observations",
            "--table",
            "cbs_observations_flat_83625ned",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 1 Gold rows\n"
    assert calls["read_table_name"] == "cbs_observations_flat_83625ned"
    assert calls["read_namespace"] is None
    assert calls["table_name"] == "cbs_observations"
    silver_rows = cast(list[dict[str, object]], calls["silver_rows"])
    assert len(silver_rows) == 1
    sink = calls["sink"]
    assert isinstance(sink, FakeGoldSink)
    assert sink.table_name == "cbs_observations"


def test_inspect_gold_command_prints_row_count_and_rows(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeConnection:
        def close(self) -> None:
            calls["closed"] = True

    def fake_connect_postgres() -> FakeConnection:
        calls["connected"] = True
        return FakeConnection()

    def fake_count_gold_rows(conn: object, table_name: str, schema: str | None) -> int:
        calls["count_table_name"] = table_name
        calls["count_schema"] = schema
        return 3

    def fake_sample_gold_rows(
        conn: object,
        table_name: str,
        schema: str | None,
        limit: int,
    ) -> list[tuple[str, str]]:
        calls["sample_table_name"] = table_name
        calls["sample_schema"] = schema
        calls["sample_limit"] = limit
        return [("gold-row-1", "value-1")]

    monkeypatch.setattr(cli, "connect_postgres", fake_connect_postgres)
    monkeypatch.setattr(cli, "count_gold_rows", fake_count_gold_rows)
    monkeypatch.setattr(cli, "sample_gold_rows", fake_sample_gold_rows)

    result = runner.invoke(
        cli.app,
        [
            "inspect-gold",
            "--table",
            "cbs_observations",
            "--limit",
            "1",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Row count: 3\nSample rows (1):\n('gold-row-1', 'value-1')\n"
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations"
    assert calls["count_schema"] is None
    assert calls["sample_table_name"] == "cbs_observations"
    assert calls["sample_schema"] is None
    assert calls["sample_limit"] == 1
    assert calls["closed"] is True
