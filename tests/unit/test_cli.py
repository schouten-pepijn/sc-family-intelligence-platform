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
