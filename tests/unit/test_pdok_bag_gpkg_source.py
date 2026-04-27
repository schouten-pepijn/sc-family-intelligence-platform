from pathlib import Path

from fip.ingestion.pdok_bag.gpkg_source import PDOKBAGGeoPackageSource


def test_gpkg_source_yields_verblijfsobject_contract() -> None:
    source = PDOKBAGGeoPackageSource(
        run_id="run-001",
        source_ref=Path("data/pdok-bag/bag-light.gpkg"),
    )

    records = list(source.iter_records())

    assert records
    record = records[0]
    assert record.source_name == "bag_gpkg"
    assert record.entity_name == "bag_gpkg.verblijfsobject"
    assert record.natural_key == record.payload["properties"]["identificatie"]
    assert record.payload["geometry"]["type"] == "Point"
    assert "feature_id" in record.payload["properties"]
