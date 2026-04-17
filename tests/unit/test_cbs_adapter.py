from fip.ingestion.cbs.adapter import CBSODataSource


def test_cbs_source_initializes_with_expected_values() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    assert source.name == "cbs_statline"
    assert source.schema_version == "v1"
    assert source.table_id == "83625NED"
    assert source.run_id == "run-001"
    assert source.base_url == "https://datasets.cbs.nl/odata/v1/CBS/83625NED"
