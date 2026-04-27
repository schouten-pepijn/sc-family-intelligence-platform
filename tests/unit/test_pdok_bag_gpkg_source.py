from pathlib import Path

import pytest

from fip.ingestion.pdok_bag.gpkg_source import PDOKBAGGeoPackageSource


def test_gpkg_source_yields_verblijfsobject_contract() -> None:
    source = PDOKBAGGeoPackageSource(
        run_id="run-001",
        source_ref=Path("data/pdok-bag/bag-light.gpkg"),
        max_features=1,
    )

    records = list(source.iter_records())

    assert records
    record = records[0]
    assert record.source_name == "bag_gpkg"
    assert record.entity_name == "bag_gpkg.verblijfsobject"
    assert record.natural_key == record.payload["properties"]["identificatie"]
    assert record.payload["geometry"]["type"] == "Point"
    assert "feature_id" in record.payload["properties"]


def test_gpkg_source_honors_max_features() -> None:
    source = PDOKBAGGeoPackageSource(
        run_id="run-001",
        source_ref=Path("data/pdok-bag/bag-light.gpkg"),
        max_features=2,
    )

    records = list(source.iter_records())

    assert len(records) == 2


def test_gpkg_source_accepts_supported_layer_contracts() -> None:
    for layer in ("pand", "woonplaats", "ligplaats", "standplaats"):
        source = PDOKBAGGeoPackageSource(
            run_id="run-001",
            source_ref=Path("data/pdok-bag/bag-light.gpkg"),
            layer=layer,
        )

        assert source.layer == layer


def test_gpkg_source_rejects_unknown_layer() -> None:
    with pytest.raises(ValueError, match="Unsupported BAG GPKG layer"):
        PDOKBAGGeoPackageSource(
            run_id="run-001",
            source_ref=Path("data/pdok-bag/bag-light.gpkg"),
            layer="adres",
        )
