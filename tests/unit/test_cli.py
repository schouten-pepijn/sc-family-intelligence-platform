from typer.testing import CliRunner

from fip import cli

runner = CliRunner()


def test_ingest_cbs_command_invokes_service_and_prints_result(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeSource:
        def __init__(self, table_id: str, run_id: str) -> None:
            calls["table_id"] = table_id
            calls["run_id"] = run_id

    class FakeSink:
        def __init__(self, table_ident: str) -> None:
            calls["target_table"] = table_ident

    def fake_ingest_source_to_sink(source, sink) -> int:
        calls["source"] = source
        calls["sink"] = sink
        return 7

    monkeypatch.setattr(cli, "CBSODataSource", FakeSource)
    monkeypatch.setattr(cli, "IcebergSink", FakeSink)
    monkeypatch.setattr(cli, "ingest_source_to_sink", fake_ingest_source_to_sink)

    result = runner.invoke(
        cli.app,
        [
            "ingest-cbs",
            "--table-id",
            "83625NED",
            "--run-id",
            "run-001",
            "--target-table",
            "bronze.cbs.observations_83625ned",
        ],
    )

    assert result.exit_code == 0
    assert result.stdout == "Wrote 7 records to bronze.cbs.observations_83625ned\n"
    assert calls["table_id"] == "83625NED"
    assert calls["run_id"] == "run-001"
    assert calls["target_table"] == "bronze.cbs.observations_83625ned"
