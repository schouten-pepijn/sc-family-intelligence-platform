from datetime import datetime, timezone

import pytest

from fip.gold.reference_codes import ReferenceCodeWriter, build_reference_row
from fip.ingestion.base import RawRecord


def make_raw_record(entity_name: str, payload: dict[str, object]) -> RawRecord:
    return RawRecord(
        source_name="cbs_statline",
        entity_name=entity_name,
        natural_key=str(payload["Identifier"]),
        retrieved_at=datetime(2026, 4, 18, 9, 0, tzinfo=timezone.utc),
        run_id="run-001",
        payload=payload,
        schema_version="v1",
        http_status=200,
    )


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


def test_build_reference_row_maps_measure_payload() -> None:
    record = make_raw_record(
        "83625NED.MeasureCodes",
        {
            "Identifier": "M001534",
            "Index": 2,
            "Title": "Gemiddelde verkoopprijs",
            "Description": "De gemiddelde verkoopprijs...",
            "MeasureGroupId": None,
            "DataType": "Long",
            "Unit": "euro",
            "Decimals": 0,
            "PresentationType": "Relative",
        },
    )

    row = build_reference_row(record)

    assert row["natural_key"] == "M001534"
    assert row["identifier"] == "M001534"
    assert row["title"] == "Gemiddelde verkoopprijs"
    assert row["measure_group_id"] is None
    assert row["data_type"] == "Long"
    assert row["unit"] == "euro"
    assert row["decimals"] == 0
    assert row["presentation_type"] == "Relative"


def test_build_reference_row_maps_period_payload_and_derives_year() -> None:
    record = make_raw_record(
        "83625NED.PeriodenCodes",
        {
            "Identifier": "1995JJ00",
            "Index": 1,
            "Title": "1995",
            "Description": "",
            "DimensionGroupId": "0",
            "Status": "Definitief",
        },
    )

    row = build_reference_row(record)

    assert row["natural_key"] == "1995JJ00"
    assert row["period_year"] == 1995
    assert row["status"] == "Definitief"
    assert row["dimension_group_id"] == "0"


def test_reference_writer_rejects_mixed_entities(monkeypatch) -> None:
    writer = ReferenceCodeWriter(table_name="cbs_measure_codes", entity="MeasureCodes")
    rows = [
        make_raw_record(
            "83625NED.MeasureCodes",
            {
                "Identifier": "M001534",
                "Title": "Gemiddelde verkoopprijs",
                "Description": "De gemiddelde verkoopprijs...",
                "MeasureGroupId": None,
                "DataType": "Long",
                "Unit": "euro",
                "Decimals": 0,
                "PresentationType": "Relative",
            },
        ),
        make_raw_record(
            "83625NED.PeriodenCodes",
            {
                "Identifier": "1995JJ00",
                "Title": "1995",
                "Description": "",
                "DimensionGroupId": "0",
                "Status": "Definitief",
            },
        ),
    ]

    with pytest.raises(ValueError, match="cannot accept record"):
        writer.write(rows)


def test_reference_writer_writes_measure_rows(monkeypatch) -> None:
    conn = FakeConnection()
    writer = ReferenceCodeWriter(table_name="cbs_measure_codes", entity="MeasureCodes")
    rows = [
        make_raw_record(
            "83625NED.MeasureCodes",
            {
                "Identifier": "M001534",
                "Index": 2,
                "Title": "Gemiddelde verkoopprijs",
                "Description": "De gemiddelde verkoopprijs...",
                "MeasureGroupId": None,
                "DataType": "Long",
                "Unit": "euro",
                "Decimals": 0,
                "PresentationType": "Relative",
            },
        )
    ]

    monkeypatch.setattr(writer, "_connect", lambda: conn)

    written = writer.write(rows)

    assert written == 1
    assert conn.committed is True
    assert conn.rolled_back is False
    assert conn.closed is True
    assert any("CREATE SCHEMA IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("CREATE TABLE IF NOT EXISTS" in statement for statement in conn.statements)
    assert any("TRUNCATE TABLE" in statement for statement in conn.statements)
    assert len(conn.executemany_calls) == 1
    sql, params_seq = conn.executemany_calls[0]
    assert "INSERT INTO" in sql
    assert len(params_seq) == 1
    assert params_seq[0][2] == "M001534"
