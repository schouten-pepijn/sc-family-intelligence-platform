from datetime import datetime, timezone
from typing import Iterator

import httpx

from fip.ingestion.base import RawRecord


class CBSODataSource:
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
        # CBS OData pulls are full-table at this stage, so `since` is intentionally unused for now.
        _ = since
        url = f"{self.base_url}/Observations"

        while url:
            data = self._get(url)

            for row in data.get("value", []):
                yield RawRecord(
                    source_name=self.name,
                    entity_name=f"{self.table_id}.Observations",
                    natural_key=str(row.get("Id", row)),
                    retrieved_at=datetime.now(timezone.utc),
                    run_id=self.run_id,
                    payload=row,
                    schema_version=self.schema_version,
                )

            url = data.get("@odata.nextLink")

    def healthcheck(self) -> bool:
        try:
            self._get(self.base_url)
            return True
        except httpx.HTTPError:
            return False

    def _get(self, url: str) -> dict:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.json()
