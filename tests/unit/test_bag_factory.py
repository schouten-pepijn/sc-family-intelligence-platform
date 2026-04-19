from fip.lakehouse.bronze.bag_factory import BAGIcebergSinkFactory
from fip.lakehouse.bronze.writer import IcebergSink


def test_bag_iceberg_sink_factory_routes_verblijfsobject_to_expected_table() -> None:
    factory = BAGIcebergSinkFactory()

    sink = factory.for_entity("bag.verblijfsobject")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.bag_verblijfsobject"


def test_bag_iceberg_sink_factory_raises_for_unknown_entity_name() -> None:
    factory = BAGIcebergSinkFactory()

    try:
        factory.for_entity("bag.unknown")
    except ValueError as exc:
        assert str(exc) == "Unknown entity name 'bag.unknown'. No mapping to table name found."
    else:
        raise AssertionError("Expected ValueError for unknown BAG entity name")
