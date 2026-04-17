import json
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

from fip.ingestion.cbs.adapter import CBSODataSource


def load_fixture(name: str) -> dict:
    fixture_path = Path("tests/fixtures/cbs") / name
    return json.loads(fixture_path.read_text(encoding="utf-8"))


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
        load_fixture("observations_single_page.json"),
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

    responses = [
        load_fixture("observations_paginated_page_1.json"),
        load_fixture("observations_paginated_page_2.json"),
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
    mock_get.assert_any_call("https://example.test/observations-page-2")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")


def test_cbs_source_iter_records_fetches_multiple_entities() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    responses = [
        load_fixture("observations_paginated_page_2.json"),
        load_fixture("measure_codes_single_page.json"),
        {"value": [{"Id": 3, "Title": "2024JJ00"}]},
        {"value": [{"Id": 4, "Title": "Amsterdam"}]},
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert len(records) == 5

    assert records[0].entity_name == "83625NED.Observations"
    assert records[1].entity_name == "83625NED.MeasureCodes"
    assert records[2].entity_name == "83625NED.MeasureCodes"
    assert records[3].entity_name == "83625NED.PeriodenCodes"
    assert records[4].entity_name == "83625NED.RegioSCodes"

    assert records[0].natural_key == "2"
    assert records[1].natural_key == "10"
    assert records[2].natural_key == "11"
    assert records[3].natural_key == "3"
    assert records[4].natural_key == "4"

    assert mock_get.call_count == 4
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")


def test_cbs_source_iter_records_handles_pagination_across_entities() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    responses = [
        load_fixture("observations_paginated_page_1.json"),
        load_fixture("observations_paginated_page_2.json"),
        load_fixture("measure_codes_single_page.json"),
        {
            "value": [{"Id": 4, "Title": "2024JJ00"}],
        },
        {
            "value": [{"Id": 5, "Title": "Amsterdam"}],
        },
    ]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert len(records) == 6

    assert records[0].entity_name == "83625NED.Observations"
    assert records[1].entity_name == "83625NED.Observations"
    assert records[2].entity_name == "83625NED.MeasureCodes"
    assert records[3].entity_name == "83625NED.MeasureCodes"
    assert records[4].entity_name == "83625NED.PeriodenCodes"
    assert records[5].entity_name == "83625NED.RegioSCodes"

    assert records[0].natural_key == "1"
    assert records[1].natural_key == "2"
    assert records[2].natural_key == "10"
    assert records[3].natural_key == "11"
    assert records[4].natural_key == "4"
    assert records[5].natural_key == "5"

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


def test_cbs_source_get_retries_and_eventually_succeeds() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"value": []}

    with patch("fip.ingestion.cbs.adapter.httpx.Client.get") as mock_get:
        mock_get.side_effect = [
            httpx.ConnectError("temporary failure"),
            httpx.ConnectError("temporary failure"),
            response,
        ]

        result = source._get("https://example.test/data")

    assert result == {"value": []}
    assert mock_get.call_count == 3


def test_cbs_source_get_raises_after_max_retries() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")

    with patch(
        "fip.ingestion.cbs.adapter.httpx.Client.get",
        side_effect=httpx.ConnectError("temporary failure"),
    ) as mock_get:
        try:
            source._get("https://example.test/data")
        except httpx.ConnectError as exc:
            assert str(exc) == "temporary failure"
        else:
            raise AssertionError("Expected ConnectError after retry exhaustion")

    assert mock_get.call_count == 3


def test_cbs_source_build_raw_record_maps_fields_correctly() -> None:
    source = CBSODataSource(table_id="83625NED", run_id="run-001")
    row = {"Id": 1, "Value": 123}

    record = source._build_raw_record("Observations", row)

    assert record.source_name == "cbs_statline"
    assert record.entity_name == "83625NED.Observations"
    assert record.natural_key == "1"
    assert isinstance(record.retrieved_at, datetime)
    assert record.retrieved_at.tzinfo is not None
    assert record.run_id == "run-001"
    assert record.payload == row
    assert record.schema_version == "v1"
    assert record.http_status == 200
