import pyarrow as pa

from fip.lakehouse.silver.cbs.cbs_observations import to_silver_observation_row
from fip.lakehouse.silver.core.sink import SilverIcebergSink


class CBSObservationSink(SilverIcebergSink):
    """Writes transformed CBS observations to Iceberg tables in Silver."""

    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]:
        return to_silver_observation_row(row)

    def _get_arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("source_name", pa.string(), nullable=False),
                pa.field("natural_key", pa.string(), nullable=False),
                pa.field("retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("schema_version", pa.string(), nullable=False),
                pa.field("http_status", pa.int32(), nullable=False),
                pa.field("observation_id", pa.int64(), nullable=False),
                pa.field("measure_code", pa.string(), nullable=False),
                pa.field("period_code", pa.string(), nullable=False),
                pa.field("region_code", pa.string(), nullable=False),
                pa.field("numeric_value", pa.float64(), nullable=True),
                pa.field("value_attribute", pa.string(), nullable=True),
                pa.field("string_value", pa.string(), nullable=True),
            ]
        )
