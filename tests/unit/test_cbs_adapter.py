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


def test_cbs_source_iter_records_returns_raw_records_for_observations() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    payload = {
        "value": [
            {"Id": 1, "Measure": "A", "Value": 123},
            {"Id": 2, "Measure": "B", "Value": 456},
        ]
    }

    with patch.object(source, "_get", return_value=payload) as mock_get:
        records = list(source.iter_records())

    mock_get.assert_called_once_with(f"{source.base_url}/Observations")
    assert len(records) == 2
    assert records[0].source_name == "cbs_statline"
    assert records[0].entity_name == "83625NED.Observations"
    assert records[0].natural_key == "1"
    assert records[0].run_id == "run-001"
    assert records[0].payload == {"Id": 1, "Measure": "A", "Value": 123}
    assert records[0].schema_version == "v1"
