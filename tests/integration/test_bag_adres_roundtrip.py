from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from fip.gold.core.service import write_rows_to_sink
from fip.gold.pdok_bag.bag_adressen_writer import BAGAdressenLandingWriter
from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.ingestion.base import RawRecord
from fip.ingestion.service import ingest_source_to_sink
from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.silver.pdok_bag.bag_adressen_service import (
    write_bronze_rows_to_bag_adressen_sink,
)
from fip.lakehouse.silver.pdok_bag.bag_adressen_sink import BAGAdressenSink
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    connect,
    count_rows,
    load_extensions,
    sample_rows,
)

pytestmark = pytest.mark.integration


class FakeSource:
    name = "bag_pdok"
    schema_version = "v1"

    def __init__(self, records: list[RawRecord]) -> None:
        self._records = records

    def iter_records(self, since: datetime | None = None):
        _ = since
        yield from self._records

    def healthcheck(self) -> bool:
        return True


def _make_record(natural_key: str, bag_id: str) -> RawRecord:
    return RawRecord(
        source_name=FakeSource.name,
        entity_name="bag.adres",
        natural_key=natural_key,
        retrieved_at=datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        run_id="integration-roundtrip",
        payload={
            "type": "Feature",
            "id": bag_id,
            "geometry": {
                "type": "Point",
                "coordinates": [5.862878870591695, 50.99994879976323],
            },
            "properties": {
                "identificatie": "0003010000126809",
                "postcode": "9901CP",
                "huisnummer": 32,
                "huisletter": "A",
                "toevoeging": None,
                "openbare_ruimte_naam": "Steenweg",
                "woonplaats_naam": "Sittard",
                "bronhouder_identificatie": "1883",
                "bronhouder_naam": "Sittard-Geleen",
            },
        },
        schema_version=FakeSource.schema_version,
        http_status=200,
    )


@pytest.mark.skipif(
    os.getenv("FIP_RUN_INTEGRATION") != "1",
    reason="Set FIP_RUN_INTEGRATION=1 to run local lakehouse integration tests.",
)
def test_bag_adres_roundtrip_against_local_lakehouse() -> None:
    suffix = uuid4().hex[:8]
    bronze_namespace = f"bronze_it_{suffix}"
    silver_namespace = f"silver_it_{suffix}"
    bronze_table_name = "bag_adressen"
    silver_table_name = "bag_adressen_flat"
    landing_table_name = f"bag_adressen_it_{suffix}"

    source = FakeSource([_make_record(natural_key="0", bag_id="adres-1")])

    bronze_written = ingest_source_to_sink(
        source=source,
        sink_factory=BAGIcebergSinkFactory(namespace=bronze_namespace),
    )
    assert bronze_written == 1

    bronze_conn = connect()
    try:
        load_extensions(bronze_conn)
        attach_lakekeeper_catalog(bronze_conn)
        bronze_rows = (
            bronze_conn.execute(
                f"""
            SELECT *
            FROM lakekeeper_catalog.{bronze_namespace}.{bronze_table_name}
            """
            )
            .to_arrow_table()
            .to_pylist()
        )
    finally:
        bronze_conn.close()

    assert len(bronze_rows) == 1
    assert bronze_rows[0]["source_name"] == "bag_pdok"
    assert bronze_rows[0]["natural_key"] == "0"

    silver_sink = BAGAdressenSink(table_ident=f"{silver_namespace}.{silver_table_name}")
    silver_written = write_bronze_rows_to_bag_adressen_sink(bronze_rows, silver_sink)
    assert silver_written == 1
    silver_written_again = write_bronze_rows_to_bag_adressen_sink(bronze_rows, silver_sink)
    assert silver_written_again == 1

    silver_conn = connect()
    try:
        load_extensions(silver_conn)
        attach_lakekeeper_catalog(silver_conn)
        silver_count = count_rows(
            silver_conn,
            table_name=silver_table_name,
            namespace=silver_namespace,
        )
        silver_rows = sample_rows(
            silver_conn,
            table_name=silver_table_name,
            namespace=silver_namespace,
            limit=1,
        )
        silver_rows_for_gold = (
            silver_conn.execute(
                f"""
            SELECT *
            FROM lakekeeper_catalog.{silver_namespace}.{silver_table_name}
            """
            )
            .to_arrow_table()
            .to_pylist()
        )
    finally:
        silver_conn.close()

    assert silver_count == 1
    assert len(silver_rows) == 1
    assert len(silver_rows_for_gold) == 1

    row = silver_rows[0]
    assert row[0] == "bag_pdok"
    assert row[1] == "0"
    assert row[3] == "integration-roundtrip"
    assert row[4] == "v1"
    assert row[5] == 200
    assert row[6] == "adres-1"

    landing_writer = BAGAdressenLandingWriter(table_name=landing_table_name)
    landing_written = write_rows_to_sink(silver_rows_for_gold, landing_writer)
    assert landing_written == 1
    landing_written_again = write_rows_to_sink(silver_rows_for_gold, landing_writer)
    assert landing_written_again == 1

    gold_conn = connect_postgres()
    try:
        gold_count = count_gold_rows(
            gold_conn,
            table_name=landing_table_name,
        )
        gold_rows = sample_gold_rows(
            gold_conn,
            table_name=landing_table_name,
            limit=1,
        )
    finally:
        gold_conn.close()

    assert gold_count == 1
    assert len(gold_rows) == 1

    gold_row = gold_rows[0]
    assert gold_row[0] == "bag_pdok"
    assert gold_row[1] == "0"
    assert gold_row[3] == "integration-roundtrip"
    assert gold_row[4] == "v1"
    assert gold_row[5] == 200
    assert gold_row[6] == "adres-1"
