from collections.abc import Iterator
from datetime import datetime, timezone

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from fip.ingestion.base import RawRecord


class PDOKBAGSource:
    # Adapter for the PDOK BAG OGC API (Dutch national address registry)
    COLLECTION = ("verblijfsobject", "pand", "adres", "adressen")

    name = "bag_pdok"
    schema_version = "v1"

    def __init__(
        self,
        run_id: str,
        collection: str = "verblijfsobject",
    ) -> None:
        if collection not in self.COLLECTION:
            raise ValueError(f"Invalid collection '{collection}'. Must be one of {self.COLLECTION}")
        self.run_id = run_id
        self.collection = "adres" if collection == "adressen" else collection
        self.base_url = "https://api.pdok.nl/kadaster/bag/ogc/v2"

    def iter_records(
        self,
        since: datetime | None = None,
    ) -> Iterator[RawRecord]:
        # Full-table pulls only; incremental sync is not implemented yet.
        _ = since
        url: str | None = f"{self.base_url}/collections/{self.collection}/items?limit=1000"

        while url:
            data = self._get(url)

            for feature in data.get("features", []):
                yield self._build_raw_record(feature)

            url = self._next_link(data)

    def healthcheck(self) -> bool:
        try:
            self._get(f"{self.base_url}/collections/{self.collection}")
            return True
        except httpx.HTTPError:
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, url: str) -> dict:
        # Retry transient network failures with exponential backoff.
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url)
            response.raise_for_status()
            return response.json()

    def _next_link(self, data: dict) -> str | None:
        for link in data.get("links", []):
            if link.get("rel") == "next" and link.get("href"):
                return str(link["href"])
        return None

    def _build_raw_record(self, feature: dict) -> RawRecord:
        feature_id = feature.get("id")
        if feature_id is None:
            raise ValueError("BAG feature is missing 'id'")

        return RawRecord(
            source_name=self.name,
            entity_name=f"bag.{self.collection}",
            natural_key=str(feature_id),
            retrieved_at=datetime.now(timezone.utc),
            run_id=self.run_id,
            payload=feature,
            schema_version=self.schema_version,
        )
