from datetime import datetime, timezone
from typing import cast

import pyarrow as pa

from fip.ingestion.base import RawRecord
from fip.settings import Settings
from fip.sink.iceberg_sink import BRONZE_ROW_FIELDS, IcebergSink


def make_raw_record(natural_key: str) -> RawRecord:
    return RawRecord(
        source_name="cbs_statline",
        entity_name="83625NED.Observations",
        natural_key=natural_key,
        retrieved_at=datetime.now(timezone.utc),
        run_id="run-001",
        payload={"Id": natural_key},
        schema_version="v1",
    )


def test_iceberg_sink_initializes_with_table_ident() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")

    assert sink.table_ident == "bronze.cbs_observations_83625ned"
    assert sink.last_written == []
    assert sink.last_written_rows == []


def test_iceberg_sink_write_returns_number_of_records() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [make_raw_record("1"), make_raw_record("2")]
    calls: dict[str, object] = {}

    class FakeTable:
        def append(
            self,
            df: pa.Table,
            snapshot_properties: dict[str, str] | None = None,
            branch: str | None = None,
        ) -> None:
            calls["df"] = df
            calls["snapshot_properties"] = snapshot_properties
            calls["branch"] = branch

    sink._load_catalog = lambda: object()  # type: ignore[method-assign]
    sink._ensure_table = lambda catalog, arrow_schema: FakeTable()  # type: ignore[method-assign]

    written = sink.write(records)

    assert written == 2
    assert sink.last_written == records
    assert len(sink.last_written_rows) == 2
    assert sink.last_written_rows[0]["source_name"] == "cbs_statline"
    assert sink.last_written_rows[0]["entity_name"] == "83625NED.Observations"
    assert sink.last_written_rows[0]["natural_key"] == "1"
    assert sink.last_written_rows[0]["run_id"] == "run-001"
    assert sink.last_written_rows[0]["schema_version"] == "v1"
    assert sink.last_written_rows[0]["http_status"] == 200
    assert sink.last_written_rows[0]["payload"] == '{"Id": "1"}'


def test_iceberg_sink_write_returns_zero_for_empty_input() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")

    written = sink.write([])

    assert written == 0
    assert sink.last_written == []
    assert sink.last_written_rows == []


def test_iceberg_sink_write_raises_for_mixed_entity_names() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [
        make_raw_record("1"),
        RawRecord(
            source_name="cbs_statline",
            entity_name="83625NED.MeasureCodes",
            natural_key="2",
            retrieved_at=datetime.now(timezone.utc),
            run_id="run-001",
            payload={"Id": 2},
            schema_version="v1",
        ),
    ]

    try:
        sink.write(records)
    except ValueError as exc:
        assert str(exc) == "IcebergSink.write expects records for a single entity"
    else:
        raise AssertionError("Expected ValueError for mixed entity names")


def test_iceberg_sink_serialized_row_has_expected_fields() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")

    row = sink._serialize_record(make_raw_record("1"))

    assert tuple(row.keys()) == BRONZE_ROW_FIELDS


def test_iceberg_sink_to_arrow_table_returns_expected_row_count_and_columns() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [make_raw_record("1"), make_raw_record("2")]

    rows = [sink._serialize_record(record) for record in records]
    table = sink._to_arrow_table(rows)

    assert table.num_rows == 2
    assert table.column_names == list(BRONZE_ROW_FIELDS)


def test_iceberg_sink_to_arrow_table_uses_expected_schema() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [make_raw_record("1")]

    rows = [sink._serialize_record(record) for record in records]
    table = sink._to_arrow_table(rows)

    schema = sink._get_arrow_schema()
    assert table.schema == schema


def test_iceberg_sink_namespace_returns_first_identifier_part() -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")

    assert sink._namespace() == "bronze"


def test_iceberg_sink_load_catalog_uses_expected_lakekeeper_and_s3_settings(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_load_catalog(name: str, **properties: str) -> object:
        captured["name"] = name
        captured["properties"] = properties
        return object()

    monkeypatch.setattr(
        "fip.sink.iceberg_sink.get_settings",
        lambda: Settings(
            lakekeeper_catalog_uri="http://localhost:8181/catalog",
            lakekeeper_warehouse_name="local-host",
            s3_endpoint="http://192.168.1.89:9000",
            s3_access_key_id="minio",
            s3_secret_access_key="minio123",
            aws_region="local-01",
            s3_path_style_access=True,
        ),
    )
    monkeypatch.setattr("fip.sink.iceberg_sink.load_catalog", fake_load_catalog)

    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")

    catalog = sink._load_catalog()

    assert catalog is not None
    assert captured["name"] == "lakekeeper"
    assert captured["properties"] == {
        "type": "rest",
        "uri": "http://localhost:8181/catalog",
        "warehouse": "local-host",
        "s3.endpoint": "http://192.168.1.89:9000",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
        "s3.region": "local-01",
        "s3.force-virtual-addressing": "false",
    }


def test_iceberg_sink_write_loads_catalog_and_ensures_table(monkeypatch) -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [make_raw_record("1")]

    fake_catalog = object()
    calls: dict[str, object] = {}

    class FakeTable:
        def append(
            self,
            df: pa.Table,
            snapshot_properties: dict[str, str] | None = None,
            branch: str | None = None,
        ) -> None:
            calls["df"] = df
            calls["snapshot_properties"] = snapshot_properties
            calls["branch"] = branch

    def fake_load_catalog() -> object:
        calls["load_catalog_called"] = True
        return fake_catalog

    def fake_ensure_table(catalog: object, arrow_schema: pa.Schema) -> FakeTable:
        calls["catalog"] = catalog
        calls["arrow_schema"] = arrow_schema
        return FakeTable()

    monkeypatch.setattr(sink, "_load_catalog", fake_load_catalog)
    monkeypatch.setattr(sink, "_ensure_table", fake_ensure_table)

    written = sink.write(records)

    assert written == 1
    assert calls["load_catalog_called"] is True
    assert calls["catalog"] is fake_catalog
    assert calls["arrow_schema"] == sink._get_arrow_schema()
    assert calls["snapshot_properties"] == {
        "fip.run_id": "run-001",
        "fip.entity_name": "83625NED.Observations",
        "fip.source_name": "cbs_statline",
    }


def test_iceberg_sink_write_appends_arrow_table_with_snapshot_properties(
    monkeypatch,
) -> None:
    sink = IcebergSink(table_ident="bronze.cbs_observations_83625ned")
    records = [make_raw_record("1")]

    fake_catalog = object()
    calls: dict[str, object] = {}

    class FakeTable:
        def append(
            self,
            df: pa.Table,
            snapshot_properties: dict[str, str] | None = None,
            branch: str | None = None,
        ) -> None:
            calls["df"] = df
            calls["snapshot_properties"] = snapshot_properties
            calls["branch"] = branch

    def fake_load_catalog() -> object:
        return fake_catalog

    def fake_ensure_table(catalog: object, arrow_schema: pa.Schema) -> FakeTable:
        calls["catalog"] = catalog
        calls["arrow_schema"] = arrow_schema
        return FakeTable()

    monkeypatch.setattr(sink, "_load_catalog", fake_load_catalog)
    monkeypatch.setattr(sink, "_ensure_table", fake_ensure_table)

    written = sink.write(records)

    assert written == 1
    assert calls["catalog"] is fake_catalog
    assert calls["arrow_schema"] == sink._get_arrow_schema()
    arrow_table = cast(pa.Table, calls["df"])
    assert arrow_table.schema == sink._get_arrow_schema()
    assert calls["snapshot_properties"] == {
        "fip.run_id": "run-001",
        "fip.entity_name": "83625NED.Observations",
        "fip.source_name": "cbs_statline",
    }
    assert calls["branch"] is None
