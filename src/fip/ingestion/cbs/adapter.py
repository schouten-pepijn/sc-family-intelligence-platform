from datetime import datetime
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
        return iter(())

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
