from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest

from fip.commands.geo import build_bag_geo_region_mapping
from fip.commands.pdok_bag import (
    archive_bag_raw,
    build_bag_landing_adressen,
    build_bag_silver_adressen,
    ingest_bag,
)
from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.ingestion.base import RawRecord
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    connect,
    load_extensions,
)
from fip.settings import get_settings

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


class FakeArchiveSource:
    def __init__(self, run_id: str, collection: str = "adres") -> None:
        self.run_id = run_id
        self.collection = collection

    def iter_records(self, since: datetime | None = None):
        _ = since
        yield _make_record(natural_key="0", bag_id="adres-1", run_id=self.run_id)

    def healthcheck(self) -> bool:
        return True


def _make_record(natural_key: str, bag_id: str, run_id: str) -> RawRecord:
    return RawRecord(
        source_name=FakeSource.name,
        entity_name="bag.adres",
        natural_key=natural_key,
        retrieved_at=datetime(2026, 4, 19, 17, 43, 42, 77000, tzinfo=timezone.utc),
        run_id=run_id,
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
def test_bag_adres_command_flow_against_local_lakehouse(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    suffix = uuid4().hex[:8]
    run_id = f"integration-roundtrip-{suffix}"
    bronze_namespace = f"bronze_it_{suffix}"
    silver_namespace = f"silver_it_{suffix}"

    monkeypatch.setenv("FIP_BRONZE_NAMESPACE", bronze_namespace)
    monkeypatch.setenv("FIP_SILVER_NAMESPACE", silver_namespace)
    get_settings.cache_clear()

    monkeypatch.setattr("fip.commands.pdok_bag.PDOKBAGSource", FakeArchiveSource)

    archive_bag_raw(
        run_id=run_id,
        collection="adres",
        limit=1,
        target="local",
        output_dir=tmp_path,
    )
    ingest_bag(
        run_id=run_id,
        collection="adres",
        target_namespace=bronze_namespace,
        limit=1,
        progress_every=1,
        raw_target="local",
        raw_output_dir=tmp_path,
    )

    build_bag_silver_adressen(table_name="bag_adressen", namespace=bronze_namespace)

    bronze_conn = connect()
    try:
        load_extensions(bronze_conn)
        attach_lakekeeper_catalog(bronze_conn)
        bronze_rows = (
            bronze_conn.execute(
                f"""
            SELECT *
            FROM lakekeeper_catalog.{bronze_namespace}.bag_adressen
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
    assert bronze_rows[0]["entity_name"] == "bag.adres"

    silver_conn = connect()
    try:
        load_extensions(silver_conn)
        attach_lakekeeper_catalog(silver_conn)
        silver_rows = (
            silver_conn.execute(
                f"""
            SELECT *
            FROM lakekeeper_catalog.{silver_namespace}.bag_adressen_flat
            """
            )
            .to_arrow_table()
            .to_pylist()
        )
    finally:
        silver_conn.close()

    assert len(silver_rows) == 1
    silver_row = silver_rows[0]
    assert silver_row["bronhouder_identificatie"] == "1883"
    assert silver_row["gemeentecode"] == "GM1883"
    assert silver_row["gemeentenaam"] == "Sittard-Geleen"
    assert silver_row["geometry"] is not None

    build_bag_landing_adressen(table_name="bag_adressen_flat", namespace=silver_namespace)
    build_bag_geo_region_mapping(
        table_name="bag_adressen_flat",
        limit=1,
        fallback_to_locatieserver=False,
        namespace=silver_namespace,
    )

    gold_conn = connect_postgres()
    try:
        gold_count = count_gold_rows(
            gold_conn,
            table_name="bag_adressen",
        )
        gold_rows = sample_gold_rows(
            gold_conn,
            table_name="bag_adressen",
            limit=1,
        )
    finally:
        gold_conn.close()

    assert gold_count == 1
    assert len(gold_rows) == 1

    gold_row = gold_rows[0]
    assert gold_row[0] == "bag_pdok"
    assert gold_row[1] == "0"
    assert gold_row[3] == run_id
    assert gold_row[4] == "v1"
    assert gold_row[5] == 200
    assert gold_row[6] == "adres-1"

    geo_conn = connect_postgres()
    try:
        geo_count = count_gold_rows(
            geo_conn,
            table_name="bag_geo_region_mapping",
        )
        geo_rows = sample_gold_rows(
            geo_conn,
            table_name="bag_geo_region_mapping",
            limit=1,
        )
    finally:
        geo_conn.close()

    assert geo_count == 1
    assert len(geo_rows) == 1

    geo_row = geo_rows[0]
    assert geo_row[0] == "adres-1"
    assert geo_row[1] == "bag_adres"
    assert geo_row[2] == "GM1883"
    assert geo_row[3] == "bag_ogc_v2_adres"
