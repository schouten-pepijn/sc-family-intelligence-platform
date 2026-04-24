from datetime import datetime, timezone

import pytest

from fip.gold.cbs.cbs_observations_writer import (
    CBS_OBSERVATION_FIELDS,
    CBSObservationLandingWriter,
)


def make_gold_row(natural_key: str, observation_id: int) -> dict[str, object]:
    return {
        "source_name": "cbs_statline",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "observation_id": observation_id,
        "measure_code": "M001534",
        "eigendom_code": None,
        "period_code": "1995JJ00",
        "region_code": "NL01",
        "numeric_value": 93750.0,
        "value_attribute": "None",
        "string_value": None,
    }


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.execute_calls: list[tuple[str, tuple[object, ...] | None]] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> object:
        self.statements.append(str(sql))
        if params is not None:
            self.execute_calls.append((str(sql), params))
        return object()

    def cursor(self) -> "FakeConnection":
        return self

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def executemany(self, sql: str, params_seq: list[tuple[object, ...]]) -> object:
        self.executemany_calls.append((str(sql), params_seq))
        return object()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


def test_gold_observation_writer_truncates_and_inserts_rows(monkeypatch) -> None:
    conn = FakeConnection()
    writer = CBSObservationLandingWriter(table_name="cbs_observations_83625ned")
    rows = [make_gold_row("1", 1), make_gold_row("2", 2)]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 2
    assert writer.last_written_rows[0]["observation_id"] == 1
    assert writer.last_written_rows[1]["natural_key"] == "2"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.execute_calls) == 0
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 2
    assert params_seq[0][0] == "cbs_statline"
    assert params_seq[0][6] == 1
    assert params_seq[0][8] is None
    assert CBS_OBSERVATION_FIELDS == (
        "source_name",
        "natural_key",
        "retrieved_at",
        "run_id",
        "schema_version",
        "http_status",
        "observation_id",
        "measure_code",
        "eigendom_code",
        "period_code",
        "region_code",
        "numeric_value",
        "value_attribute",
        "string_value",
    )


def test_gold_observation_writer_returns_zero_for_empty_input(monkeypatch) -> None:
    conn = FakeConnection()
    writer = CBSObservationLandingWriter(table_name="cbs_observations_83625ned")

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write([])

    assert written == 0
    assert writer.last_written_rows == []
    assert conn.statements == []
    assert conn.execute_calls == []
    assert conn.executemany_calls == []
    assert conn.committed is False
    assert conn.closed is False


def test_gold_observation_writer_rejects_mixed_run_ids(monkeypatch) -> None:
    conn = FakeConnection()
    writer = CBSObservationLandingWriter(table_name="cbs_observations_83625ned")
    rows = [
        make_gold_row("1", 1),
        {
            **make_gold_row("2", 2),
            "run_id": "run-002",
        },
    ]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    with pytest.raises(ValueError, match="single run_id"):
        writer.write(rows)
