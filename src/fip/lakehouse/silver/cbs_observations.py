import json

SILVER_OBSERVATION_FIELDS = (
    "source_name",
    "natural_key",
    "retrieved_at",
    "run_id",
    "schema_version",
    "http_status",
    "observation_id",
    "measure_code",
    "period_code",
    "region_code",
    "numeric_value",
    "value_attribute",
    "string_value",
)


def to_silver_observation_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in SILVER_OBSERVATION_FIELDS}


def flatten_bronze_observation(row: dict[str, object]) -> dict[str, object]:
    payload_raw = row["payload"]
    if not isinstance(payload_raw, str):
        raise ValueError("Expected 'payload' field to be a string")

    payload = json.loads(payload_raw)

    return {
        "source_name": row["source_name"],
        "natural_key": row["natural_key"],
        "retrieved_at": row["retrieved_at"],
        "run_id": row["run_id"],
        "schema_version": row["schema_version"],
        "http_status": row["http_status"],
        "observation_id": payload["Id"],
        "measure_code": payload["Measure"],
        "period_code": payload["Perioden"],
        "region_code": payload["RegioS"],
        "numeric_value": payload["Value"],
        "value_attribute": payload["ValueAttribute"],
        "string_value": payload["StringValue"],
    }


def flatten_bronze_observation_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_observation(row) for row in rows]
