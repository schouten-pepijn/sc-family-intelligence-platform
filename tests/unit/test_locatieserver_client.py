from fip.ingestion.locatieserver.client import LocatieserverClient


def test_lookup_calls_lookup_endpoint(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"features": [{"id": "x"}]}

    class FakeClient:
        def __init__(self, timeout: float) -> None:
            calls["timeout"] = timeout

        def __enter__(self) -> "FakeClient":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, url: str, params: dict[str, str | int]) -> FakeResponse:
            calls["url"] = url
            calls["params"] = params
            return FakeResponse()

    monkeypatch.setattr("fip.ingestion.locatieserver.client.httpx.Client", FakeClient)

    client = LocatieserverClient()
    result = client.lookup("6131BE 32 Steenweg Sittard")

    assert calls["url"] == "https://api.pdok.nl/bzk/locatieserver/search/v3_1/lookup"
    assert calls["params"] == {"q": "6131BE 32 Steenweg Sittard", "rows": 10}
    assert result == {"features": [{"id": "x"}]}
