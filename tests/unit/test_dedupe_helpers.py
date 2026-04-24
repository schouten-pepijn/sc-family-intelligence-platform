from datetime import datetime, timezone

from fip.commands._helpers import dedupe_raw_records
from fip.ingestion.base import RawRecord


def make_raw_record(
    source_name: str,
    entity_name: str,
    natural_key: str,
    payload_value: str,
) -> RawRecord:
    return RawRecord(
        source_name=source_name,
        entity_name=entity_name,
        natural_key=natural_key,
        retrieved_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        run_id="run-001",
        payload={"value": payload_value},
        schema_version="v1",
        http_status=200,
    )


def test_dedupe_raw_records_keeps_last_record_per_source_entity_key() -> None:
    records = [
        make_raw_record("bag_pdok", "bag.adres", "1", "first"),
        make_raw_record("bag_pdok", "bag.adres", "1", "second"),
    ]

    deduped = dedupe_raw_records(records)

    assert len(deduped) == 1
    assert deduped[0].payload["value"] == "second"


def test_dedupe_raw_records_preserves_same_key_across_entities() -> None:
    records = [
        make_raw_record("bag_pdok", "bag.adres", "1", "adres"),
        make_raw_record("bag_pdok", "bag.pand", "1", "pand"),
    ]

    deduped = dedupe_raw_records(records)

    assert len(deduped) == 2
