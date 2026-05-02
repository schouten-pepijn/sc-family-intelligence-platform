from datetime import datetime, timezone
from typing import Iterator

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from fip.ingestion.base import RawRecord


class CBSODataSource:
    # Adapter for CBS OData API (Dutch statistics bureau)
    ENTITIES = ("Observations", "MeasureCodes", "PeriodenCodes", "RegioSCodes")
    EXTRA_ENTITIES_BY_TABLE_ID = {
        "85036NED": ("EigendomCodes",),
        "83648NED": ("SoortMisdrijfCodes",),
    }
    ENTITY_KEY_FIELDS = {
        "Observations": ("Id",),
        "MeasureCodes": ("Id", "Key", "Identifier"),
        "PeriodenCodes": ("Id", "Key", "Identifier"),
        "RegioSCodes": ("Id", "Key", "Identifier"),
        "EigendomCodes": ("Id", "Key", "Identifier"),
        "SoortMisdrijfCodes": ("Id", "Key", "Identifier"),
    }

    name = "cbs_statline"
    schema_version = "v1"

    def __init__(
        self,
        table_id: str,
        run_id: str,
    ) -> None:
        self.table_id = table_id
        self.run_id = run_id
        self.base_url = f"https://datasets.cbs.nl/odata/v1/CBS/{table_id}"

    def iter_records(self, since: datetime | None = None) -> Iterator[RawRecord]:
        # Full-table pulls only; incremental sync not yet implemented
        _ = since
        for entity in self.ENTITIES + self.EXTRA_ENTITIES_BY_TABLE_ID.get(self.table_id, ()):
            url: str | None = f"{self.base_url}/{entity}"

            while url:
                data = self._get(url)

                for row in data.get("value", []):
                    yield self._build_raw_record(entity, row)

                # Follow OData pagination links until exhausted
                url = data.get("@odata.nextLink")

    def healthcheck(self) -> bool:
        try:
            self._get(self.base_url)
            return True
        except httpx.HTTPError:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, url: str) -> dict:
        # Retry transient network failures with exponential backoff
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.json()

    def _natural_key_for_row(self, entity: str, row: dict) -> str:
        for field in self.ENTITY_KEY_FIELDS.get(entity, ("Id", "Key", "Identifier")):
            record_id = row.get(field)
            if record_id is not None:
                return str(record_id)
        raise ValueError(
            f"Missing natural key for CBS entity '{entity}'. Tried fields: "
            f"{', '.join(self.ENTITY_KEY_FIELDS.get(entity, ('Id', 'Key', 'Identifier')))}"
        )

    def _build_raw_record(self, entity: str, row: dict) -> RawRecord:
        return RawRecord(
            source_name=self.name,
            entity_name=f"{self.table_id}.{entity}",
            natural_key=self._natural_key_for_row(entity, row),
            retrieved_at=datetime.now(timezone.utc),
            run_id=self.run_id,
            payload=row,
            schema_version=self.schema_version,
        )
