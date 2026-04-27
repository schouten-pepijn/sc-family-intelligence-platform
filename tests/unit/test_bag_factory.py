from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.bronze.writer import IcebergSink


def test_bag_iceberg_sink_factory_routes_verblijfsobject_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag.verblijfsobject")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_verblijfsobject"


def test_bag_iceberg_sink_factory_routes_bag_gpkg_verblijfsobject_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag_gpkg.verblijfsobject")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_gpkg_verblijfsobject"


def test_bag_iceberg_sink_factory_routes_bag_gpkg_layers_to_expected_tables() -> None:
    factory = BAGIcebergSinkFactory()

    expected_tables = {
        "bag_gpkg.pand": "bronze.bag_gpkg_pand",
        "bag_gpkg.woonplaats": "bronze.bag_gpkg_woonplaats",
        "bag_gpkg.ligplaats": "bronze.bag_gpkg_ligplaats",
        "bag_gpkg.standplaats": "bronze.bag_gpkg_standplaats",
    }

    for entity_name, table_ident in expected_tables.items():
        sink = factory.for_entity(entity_name)

        assert isinstance(sink, IcebergSink)
        assert sink.table_ident == table_ident


def test_bag_iceberg_sink_factory_routes_pand_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag.pand")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_pand"


def test_bag_iceberg_sink_factory_routes_adres_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag.adres")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_adressen"


def test_bag_iceberg_sink_factory_routes_legacy_adressen_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag.adressen")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_adressen"


def test_bag_iceberg_sink_factory_raises_for_unknown_entity_name() -> None:
    factory = BAGIcebergSinkFactory()

    try:
        factory.for_entity("bag.unknown")
    except ValueError as exc:
        assert str(exc) == "Unknown entity name 'bag.unknown'. No mapping to table name found."
    else:
        raise AssertionError("Expected ValueError for unknown BAG entity name")
