from unittest.mock import patch

import httpx

from fip.ingestion.cbs.adapter import CBSODataSource


def test_cbs_source_initializes_with_expected_values() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    assert source.name == "cbs_statline"
    assert source.schema_version == "v1"
    assert source.table_id == "83625NED"
    assert source.run_id == "run-001"
    assert source.base_url == "https://datasets.cbs.nl/odata/v1/CBS/83625NED"


def test_cbs_source_healthcheck_returns_true_when_get_succeeds() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    with patch.object(source, "_get", return_value={"value": []}):
        assert source.healthcheck() is True


def test_cbs_source_healthcheck_returns_false_when_get_fails() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    with patch.object(source, "_get", side_effect=httpx.HTTPError("boom")):
        assert source.healthcheck() is False


def test_cbs_source_iter_records_is_empty_for_now() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    assert list(source.iter_records()) == []
