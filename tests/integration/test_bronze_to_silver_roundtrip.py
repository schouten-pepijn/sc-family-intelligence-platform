from __future__ import annotations

import os
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from fip.gold.readback import connect as connect_postgres
from fip.gold.readback import count_rows as count_gold_rows
from fip.gold.readback import sample_rows as sample_gold_rows
from fip.gold.service import write_silver_rows_to_gold_sink
from fip.gold.writer import GoldObservationWriter
from fip.ingestion.base import RawRecord
from fip.ingestion.service import ingest_source_to_sink
from fip.lakehouse.bronze.cbs_factory import CBSIcebergSinkFactory
from fip.lakehouse.silver.cbs_observations_service import (
    write_bronze_rows_to_cbs_observation_sink,
)
from fip.lakehouse.silver.cbs_observations_sink import CBSObservationSink
from fip.readback.duckdb import (
    attach_lakekeeper_catalog,
    connect,
    count_rows,
    load_extensions,
    sample_rows,
)

pytestmark = pytest.mark.integration


class FakeSource:
    name = "cbs_statline"
    schema_version = "v1"

    def __init__(self, records: list[RawRecord]) -> None:
        self._records = records

    def iter_records(self, since: datetime | None = None):
        _ = since
        yield from self._records

    def healthcheck(self) -> bool:
        return True


def _make_record(table_id: str, natural_key: str, payload: dict[str, object]) -> RawRecord:
    return RawRecord(
        source_name=FakeSource.name,
        entity_name=f"{table_id}.Observations",
        natural_key=natural_key,
        retrieved_at=datetime(2026, 4, 18, 0, 36, 49, 362000, tzinfo=timezone.utc),
        run_id="integration-roundtrip",
        payload=payload,
        schema_version=FakeSource.schema_version,
    )


@pytest.mark.skipif(
    os.getenv("FIP_RUN_INTEGRATION") != "1",
    reason="Set FIP_RUN_INTEGRATION=1 to run local lakehouse integration tests.",
)
def test_bronze_to_silver_roundtrip_against_local_lakehouse() -> None:
    suffix = uuid4().hex[:8]
    table_id = f"ITRT{suffix}".upper()
    bronze_namespace = f"bronze_it_{suffix}"
    silver_namespace = f"silver_it_{suffix}"
    bronze_table_name = f"cbs_observations_{table_id.lower()}"
    silver_table_name = f"cbs_observations_flat_{table_id.lower()}"
    gold_table_name = f"cbs_observations_it_{table_id.lower()}"

    source = FakeSource(
        [
            _make_record(
                table_id=table_id,
                natural_key="0",
                payload={
                    "Id": 0,
                    "Measure": "M001534",
                    "Perioden": "1995JJ00",
                    "RegioS": "NL01",
                    "StringValue": None,
                    "Value": 93750.0,
                    "ValueAttribute": "None",
                },
            )
        ]
    )

    bronze_written = ingest_source_to_sink(
        source=source,
        sink_factory=CBSIcebergSinkFactory(namespace=bronze_namespace),
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
    assert bronze_rows[0]["source_name"] == "cbs_statline"
    assert bronze_rows[0]["natural_key"] == "0"

    silver_sink = CBSObservationSink(
        table_ident=f"{silver_namespace}.{silver_table_name}",
    )
    silver_written = write_bronze_rows_to_cbs_observation_sink(bronze_rows, silver_sink)
    assert silver_written == 1
    silver_written_again = write_bronze_rows_to_cbs_observation_sink(
        bronze_rows,
        silver_sink,
    )
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
    assert row[0] == "cbs_statline"
    assert row[1] == "0"
    assert row[3] == "integration-roundtrip"
    assert row[4] == "v1"
    assert row[5] == 200
    assert row[6] == 0
    assert row[7] == "M001534"
    assert row[8] == "1995JJ00"
    assert row[9] == "NL01"
    assert row[10] == 93750.0
    assert row[11] == "None"
    assert row[12] is None

    gold_writer = GoldObservationWriter(table_name=gold_table_name)
    gold_written = write_silver_rows_to_gold_sink(silver_rows_for_gold, gold_writer)
    assert gold_written == 1
    gold_written_again = write_silver_rows_to_gold_sink(silver_rows_for_gold, gold_writer)
    assert gold_written_again == 1

    gold_conn = connect_postgres()
    try:
        gold_count = count_gold_rows(
            gold_conn,
            table_name=gold_table_name,
        )
        gold_rows = sample_gold_rows(
            gold_conn,
            table_name=gold_table_name,
            limit=1,
        )
    finally:
        gold_conn.close()

    assert gold_count == 1
    assert len(gold_rows) == 1

    gold_row = gold_rows[0]
    assert gold_row[0] == "cbs_statline"
    assert gold_row[1] == "0"
    assert gold_row[3] == "integration-roundtrip"
    assert gold_row[4] == "v1"
    assert gold_row[5] == 200
    assert gold_row[6] == 0
    assert gold_row[7] == "M001534"
    assert gold_row[8] == "1995JJ00"
    assert gold_row[9] == "NL01"
    assert gold_row[10] == 93750.0
    assert gold_row[11] == "None"
    assert gold_row[12] is None
