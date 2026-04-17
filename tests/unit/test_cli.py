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
        "Row count: 23095\n"
        "Sample rows (2):\n"
        "('row-1', 'value-1')\n"
        "('row-2', 'value-2')\n"
    )
    assert calls["connected"] is True
    assert calls["count_table_name"] == "cbs_observations_83625ned"
    assert calls["count_namespace"] is None
    assert calls["sample_table_name"] == "cbs_observations_83625ned"
    assert calls["sample_namespace"] is None
    assert calls["sample_limit"] == 2
    assert calls["closed"] is True
