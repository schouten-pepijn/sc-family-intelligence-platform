from __future__ import annotations

from unittest.mock import patch

from fip.ingestion.cbs_crime.adapter import CBSCrimeSource


def test_cbs_crime_source_initializes_with_expected_values() -> None:
    source = CBSCrimeSource(run_id="run-001")

    assert source.name == "cbs_crime"
    assert source.table_id == "83648NED"
    assert source.run_id == "run-001"
    assert source.base_url == "https://datasets.cbs.nl/odata/v1/CBS/83648NED"


def test_cbs_crime_source_iter_records_uses_cbs_odata_endpoints() -> None:
    source = CBSCrimeSource(run_id="run-001")

    responses = [{"value": []}, {"value": []}, {"value": []}, {"value": []}, {"value": []}]

    with patch.object(source, "_get", side_effect=responses) as mock_get:
        records = list(source.iter_records())

    assert records == []
    assert mock_get.call_count == 5
    mock_get.assert_any_call(f"{source.base_url}/Observations")
    mock_get.assert_any_call(f"{source.base_url}/MeasureCodes")
    mock_get.assert_any_call(f"{source.base_url}/PeriodenCodes")
    mock_get.assert_any_call(f"{source.base_url}/RegioSCodes")
    mock_get.assert_any_call(f"{source.base_url}/SoortMisdrijfCodes")
