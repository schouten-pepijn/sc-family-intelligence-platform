from datetime import datetime, timezone
from typing import Iterator

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from fip.ingestion.base import RawRecord


class CBSODataSource:
    # Adapter for CBS OData API (Dutch statistics bureau)
    ENTITIES = ("Observations", "MeasureCodes", "PeriodenCodes", "RegioSCodes")

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
        for entity in self.ENTITIES:
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
        record_id = row.get("Id")
        if record_id is None:
            raise ValueError(f"Missing Id for CBS entity '{entity}'")
        return str(record_id)

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

