from typing import cast

from typer.testing import CliRunner

from fip import cli

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
    monkeypatch.setattr(cli, "IcebergSinkFactory", FakeSinkFactory)
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
