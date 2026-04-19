import pyarrow as pa

from fip.lakehouse.silver.bag_pand import to_bag_pand_row
from fip.lakehouse.silver.core.sink import SilverIcebergSink


class BAGPandSink(SilverIcebergSink):
    """Writes flattened BAG pand rows to Iceberg tables in Silver."""

    def _to_silver_row(self, row: dict[str, object]) -> dict[str, object]:
        return to_bag_pand_row(row)

    def _get_arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field("source_name", pa.string(), nullable=False),
                pa.field("natural_key", pa.string(), nullable=False),
                pa.field("retrieved_at", pa.timestamp("us", tz="UTC"), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("schema_version", pa.string(), nullable=False),
                pa.field("http_status", pa.int32(), nullable=False),
                pa.field("bag_id", pa.string(), nullable=False),
                pa.field("pand_identificatie", pa.string(), nullable=False),
                pa.field("pand_status", pa.string(), nullable=True),
                pa.field("oorspronkelijk_bouwjaar", pa.int64(), nullable=True),
                pa.field("geconstateerd", pa.string(), nullable=True),
                pa.field("documentdatum", pa.string(), nullable=True),
                pa.field("documentnummer", pa.string(), nullable=True),
                pa.field("geometry", pa.string(), nullable=True),
            ]
        )
