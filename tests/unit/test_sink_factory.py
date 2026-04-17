from fip.sink.factory import IcebergSinkFactory
from fip.sink.iceberg_sink import IcebergSink


def test_iceberg_sink_factory_routes_observations_entity_to_expected_table() -> None:
    factory = IcebergSinkFactory()

    sink = factory.for_entity("83625NED.Observations")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.cbs.observations_83625ned"


def test_iceberg_sink_factory_routes_measure_codes_entity_to_expected_table() -> None:
    factory = IcebergSinkFactory()

    sink = factory.for_entity("83625NED.MeasureCodes")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.cbs.measure_codes_83625ned"


def test_iceberg_sink_factory_raises_for_invalid_entity_name_format() -> None:
    factory = IcebergSinkFactory()

    try:
        factory.for_entity("invalid-entity-name")
    except ValueError as exc:
        assert (
            str(exc)
            == "Invalid entity name 'invalid-entity-name'. Expected format 'table_id.entity_name'."
        )
    else:
        raise AssertionError("Expected ValueError for invalid entity name format")


def test_iceberg_sink_factory_raises_for_unknown_entity_name() -> None:
    factory = IcebergSinkFactory()

    try:
        factory.for_entity("83625NED.UnknownEntity")
    except ValueError as exc:
        assert str(exc) == "Unknown entity name 'UnknownEntity'. No mapping to table name found."
    else:
        raise AssertionError("Expected ValueError for unknown entity name")
