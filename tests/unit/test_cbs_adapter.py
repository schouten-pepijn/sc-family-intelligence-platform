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

    with patch.object(source, "_get", return_value={"value": []}) as mock_get:
        assert source.healthcheck() is True

    mock_get.assert_called_once_with(source.base_url)


def test_cbs_source_healthcheck_returns_false_when_get_fails() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    with patch.object(source, "_get", side_effect=httpx.HTTPError("boom")):
        assert source.healthcheck() is False


def test_cbs_source_iter_records_returns_raw_records_for_observations() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    responses = [
        {
            "value": [
                {"Id": 1, "Measure": "A", "Value": 123},
                {"Id": 2, "Measure": "B", "Value": 456},
            ]
        },
        {"value": []},
        {"value": []},
        {"value": []},
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert mock_get.call_count == 4
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")
    assert len(records) == 2
    assert records[0].source_name == "cbs_statline"
    assert records[0].entity_name == "83625NED.Observations"
    assert records[0].natural_key == "1"
    assert records[0].run_id == "run-001"
    assert records[0].payload == {"Id": 1, "Measure": "A", "Value": 123}
    assert records[0].schema_version == "v1"


def test_cbs_source_iter_records_follows_odata_next_link() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    first_page = {
        "value": [{"Id": 1, "Value": 123}],
        "@odata.nextLink": "https://example.test/page-2",
    }
    second_page = {
        "value": [{"Id": 2, "Value": 456}],
    }

    responses = [
        first_page,
        second_page,
        {"value": []},
        {"value": []},
        {"value": []},
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert len(records) == 2
    assert records[0].natural_key == "1"
    assert records[1].natural_key == "2"

    assert mock_get.call_count == 5
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call("https://example.test/page-2")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")


def test_cbs_source_iter_records_fetches_multiple_entities() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    responses = [
        {"value": [{"Id": 1, "Value": 100}]},
        {"value": [{"Id": 2, "Title": "Measure A"}]},
        {"value": [{"Id": 3, "Title": "2024JJ00"}]},
        {"value": [{"Id": 4, "Title": "Amsterdam"}]},
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert len(records) == 4

    assert records[0].entity_name == "83625NED.Observations"
    assert records[1].entity_name == "83625NED.MeasureCodes"
    assert records[2].entity_name == "83625NED.PeriodenCodes"
    assert records[3].entity_name == "83625NED.RegioSCodes"

    assert records[0].natural_key == "1"
    assert records[1].natural_key == "2"
    assert records[2].natural_key == "3"
    assert records[3].natural_key == "4"

    assert mock_get.call_count == 4
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")


def test_cbs_source_iter_records_handles_pagination_across_entities() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    responses = [
        {
            "value": [{"Id": 1, "Value": 100}],
            "@odata.nextLink": "https://example.test/observations-page-2",
        },
        {
            "value": [{"Id": 2, "Value": 200}],
        },
        {
            "value": [{"Id": 3, "Title": "Measure A"}],
        },
        {
            "value": [{"Id": 4, "Title": "2024JJ00"}],
        },
        {
            "value": [{"Id": 5, "Title": "Amsterdam"}],
        },
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert len(records) == 5

    assert records[0].entity_name == "83625NED.Observations"
    assert records[1].entity_name == "83625NED.Observations"
    assert records[2].entity_name == "83625NED.MeasureCodes"
    assert records[3].entity_name == "83625NED.PeriodenCodes"
    assert records[4].entity_name == "83625NED.RegioSCodes"

    assert records[0].natural_key == "1"
    assert records[1].natural_key == "2"
    assert records[2].natural_key == "3"
    assert records[3].natural_key == "4"
    assert records[4].natural_key == "5"

    assert mock_get.call_count == 5
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call("https://example.test/observations-page-2")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")


def test_cbs_source_natural_key_for_row_returns_id_as_string() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    assert source._natural_key_for_row("Observations", {"Id": 1, "Value": 123}) == "1"


def test_cbs_source_natural_key_for_row_raises_when_id_is_missing() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    with patch.object(source, "_get", return_value={"value": [{"Value": 123}]}) as mock_get:
        try:
            list(source.iter_records())
        except ValueError as exc:
            assert str(exc) == "Missing Id for CBS entity 'Observations'"
        else:
            raise AssertionError("Expected ValueError when Id is missing")

    mock_get.assert_called_once_with(f"{source.base_url}/Observations")
