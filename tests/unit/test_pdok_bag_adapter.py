from fip.ingestion.pdok_bag.adapter import PDOKBAGSource


def test_pdok_bag_source_builds_raw_record_from_feature(monkeypatch) -> None:
    source = PDOKBAGSource(run_id="run-001")

    def fake_get(url: str) -> dict:
        assert url.endswith("/collections/verblijfsobject/items?limit=1000")
        return {
            "features": [
                {
                    "id": "0003010000126809",
                    "properties": {
                        "identificatie": "0003010000126809",
                        "postcode": "9901CP",
                    },
                    "geometry": None,
                }
            ],
            "links": [],
        }

    monkeypatch.setattr(source, "_get", fake_get)

    records = list(source.iter_records())

    assert len(records) == 1
    record = records[0]
    assert record.source_name == "bag_pdok"
    assert record.entity_name == "bag.verblijfsobject"
    assert record.natural_key == "0003010000126809"
    assert record.run_id == "run-001"
    assert record.schema_version == "v1"
    assert record.payload["id"] == "0003010000126809"


def test_pdok_bag_source_follows_next_link(monkeypatch) -> None:
    source = PDOKBAGSource(run_id="run-001")

    calls: list[str] = []

    def fake_get(url: str) -> dict:
        calls.append(url)
        if len(calls) == 1:
            return {
                "features": [
                    {
                        "id": "first",
                        "properties": {"postcode": "1111AA"},
                        "geometry": None,
                    }
                ],
                "links": [{"rel": "next", "href": "https://example.test/next"}],
            }

        return {
            "features": [
                {
                    "id": "second",
                    "properties": {"postcode": "2222BB"},
                    "geometry": None,
                }
            ],
            "links": [],
        }

    monkeypatch.setattr(source, "_get", fake_get)

    records = list(source.iter_records())

    assert calls == [
        "https://api.pdok.nl/kadaster/bag/ogc/v2/collections/verblijfsobject/items?limit=1000",
        "https://example.test/next",
    ]
    assert [record.natural_key for record in records] == ["first", "second"]


def test_pdok_bag_healthcheck_uses_collection_endpoint(monkeypatch) -> None:
    source = PDOKBAGSource(run_id="run-001")

    def fake_get(url: str) -> dict:
        assert url == "https://api.pdok.nl/kadaster/bag/ogc/v2/collections/verblijfsobject"
        return {}

    monkeypatch.setattr(source, "_get", fake_get)

    assert source.healthcheck() is True
