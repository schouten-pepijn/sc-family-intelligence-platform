from datetime import datetime, timezone

import pytest

from fip.gold.pdok_bag.bag_gpkg_verblijfsobject_writer import (
    BAGGpkgVerblijfsobjectLandingWriter,
)


def make_bag_gpkg_verblijfsobject_row(natural_key: str, bag_id: str) -> dict[str, object]:
    return {
        "source_name": "bag_gpkg",
        "natural_key": natural_key,
        "retrieved_at": datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        "run_id": "run-001",
        "schema_version": "v1",
        "http_status": 200,
        "bag_id": bag_id,
        "verblijfsobject_identificatie": "0000010000057469",
        "hoofdadres_identificatie": "0000200000057534",
        "postcode": "6131BE",
        "huisnummer": 32,
        "huisletter": "A",
        "toevoeging": None,
        "woonplaats_naam": "Sittard",
        "openbare_ruimte_naam": "Steenweg",
        "gebruiksdoel": "woonfunctie",
        "oppervlakte": 72,
        "geometry": '{"type": "Point"}',
    }


class FakeConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.executemany_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> object:
        self.statements.append(str(sql))
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


def test_bag_gpkg_verblijfsobject_landing_writer_truncates_and_inserts_rows(
    monkeypatch,
) -> None:
    conn = FakeConnection()
    writer = BAGGpkgVerblijfsobjectLandingWriter(table_name="bag_gpkg_verblijfsobject")
    rows = [
        make_bag_gpkg_verblijfsobject_row("1", "bag-1"),
        make_bag_gpkg_verblijfsobject_row("2", "bag-2"),
    ]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 2
    assert writer.last_written_rows[0]["bag_id"] == "bag-1"
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 2
    assert params_seq[0][0] == "bag_gpkg"


def test_bag_gpkg_verblijfsobject_landing_writer_returns_zero_for_empty_input(
    monkeypatch,
) -> None:
    conn = FakeConnection()
    writer = BAGGpkgVerblijfsobjectLandingWriter(table_name="bag_gpkg_verblijfsobject")

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write([])

    assert written == 0
    assert writer.last_written_rows == []
    assert conn.statements == []
    assert conn.executemany_calls == []
    assert conn.committed is False
    assert conn.closed is False


def test_bag_gpkg_verblijfsobject_landing_writer_rejects_mixed_run_ids() -> None:
    writer = BAGGpkgVerblijfsobjectLandingWriter(table_name="bag_gpkg_verblijfsobject")
    rows = [
        make_bag_gpkg_verblijfsobject_row("1", "bag-1"),
        {
            **make_bag_gpkg_verblijfsobject_row("2", "bag-2"),
            "run_id": "run-002",
        },
    ]

    with pytest.raises(ValueError, match="single run_id"):
        writer.write(rows)
