from datetime import datetime

from fip.ingestion.base import RawRecord


def test_raw_record_defaults_and_fields() -> None:
    retrieved_at = datetime(2026, 4, 17, 12, 0, 0)

    record = RawRecord(
        source_name="cbs_statline",
        entity_name="83625NED.Observations",
        natural_key="row-1",
        retrieved_at=retrieved_at,
        run_id="test-run-001",
        payload={"value": 123},
        schema_version="v1",
    )

    assert record.source_name == "cbs_statline"
    assert record.entity_name == "83625NED.Observations"
    assert record.natural_key == "row-1"
    assert record.retrieved_at == retrieved_at
    assert record.run_id == "test-run-001"
    assert record.payload == {"value": 123}
    assert record.schema_version == "v1"
    assert record.http_status == 200
