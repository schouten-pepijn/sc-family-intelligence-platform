from datetime import datetime, timezone

from fip.ingestion.base import RawRecord
from fip.sink.iceberg_sink import IcebergSink


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
    sink = IcebergSink(table_ident="bronze.cbs.observations_83625ned")

    assert sink.table_ident == "bronze.cbs.observations_83625ned"


def test_iceberg_sink_write_returns_number_of_records() -> None:
    sink = IcebergSink(table_ident="bronze.cbs.observations_83625ned")
    records = [make_raw_record("1"), make_raw_record("2")]

    written = sink.write(records)

    assert written == 2


def test_iceberg_sink_write_returns_zero_for_empty_input() -> None:
    sink = IcebergSink(table_ident="bronze.cbs.observations_83625ned")

    written = sink.write([])

    assert written == 0
