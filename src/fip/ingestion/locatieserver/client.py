from __future__ import annotations

from collections.abc import Mapping

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


class LocatieserverClient:
    def __init__(
        self,
        base_url: str | None = None,
    ) -> None:
        self.base_url = (base_url or "https://api.pdok.nl/bzk/locatieserver/search/v3_1").rstrip(
            "/"
        )

    def lookup(self, query: str, rows: int = 10) -> dict[str, object]:
        return self._get(
            "lookup",
            params={"q": query, "rows": rows},
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _get(self, endpoint: str, params: Mapping[str, str | int]) -> dict[str, object]:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(f"{self.base_url}/{endpoint}", params=params)
            response.raise_for_status()
            return response.json()
