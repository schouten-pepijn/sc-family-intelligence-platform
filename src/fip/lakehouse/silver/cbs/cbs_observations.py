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
    "eigendom_code",
    "period_code",
    "region_code",
    "numeric_value",
    "value_attribute",
    "string_value",
    "woningtype_code",
    "woningkenmerk_code",
)


def to_silver_observation_row(row: dict[str, object]) -> dict[str, object]:
    return {field: row[field] for field in SILVER_OBSERVATION_FIELDS}


def flatten_bronze_observation(row: dict[str, object]) -> dict[str, object]:
    """Flatten a bronze observation record by extracting and renaming nested payload fields.

    Maps CBS source field names (Id, Measure, Perioden, RegioS) to domain-level
    names for downstream consumers, decoupling them from API schema changes.
    """
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
        "eigendom_code": payload.get("Eigendom"),
        "period_code": payload["Perioden"],
        "region_code": payload["RegioS"],
        "numeric_value": payload["Value"],
        "value_attribute": payload["ValueAttribute"],
        "string_value": payload["StringValue"],
        "woningtype_code": payload.get("Woningtype"),
        "woningkenmerk_code": payload.get("Woningkenmerk"),
    }


def flatten_bronze_observation_rows(
    rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [flatten_bronze_observation(row) for row in rows]
