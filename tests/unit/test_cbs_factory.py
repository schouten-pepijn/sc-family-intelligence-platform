from fip.lakehouse.bronze.cbs_factory import CBSIcebergSinkFactory
from fip.lakehouse.bronze.writer import IcebergSink


def test_cbs_iceberg_sink_factory_routes_observations_entity_to_expected_table() -> None:
    factory = CBSIcebergSinkFactory()

    sink = factory.for_entity("83625NED.Observations")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.cbs_observations_83625ned"


def test_cbs_iceberg_sink_factory_routes_measure_codes_entity_to_expected_table() -> None:
    factory = CBSIcebergSinkFactory()

    sink = factory.for_entity("83625NED.MeasureCodes")

    assert isinstance(sink, IcebergSink)
    assert sink.table_ident == "bronze.cbs_measure_codes_83625ned"


def test_cbs_iceberg_sink_factory_raises_for_invalid_entity_name_format() -> None:
    factory = CBSIcebergSinkFactory()

    try:
        factory.for_entity("invalid-entity-name")
    except ValueError as exc:
        assert (
            str(exc)
            == "Invalid entity name 'invalid-entity-name'. Expected format 'table_id.entity_name'."
        )
    else:
        raise AssertionError("Expected ValueError for invalid entity name format")


def test_cbs_iceberg_sink_factory_raises_for_unknown_entity_name() -> None:
    factory = CBSIcebergSinkFactory()

    try:
        factory.for_entity("83625NED.UnknownEntity")
    except ValueError as exc:
        assert str(exc) == "Unknown entity name 'UnknownEntity'. No mapping to table name found."
    else:
        raise AssertionError("Expected ValueError for unknown entity name")
