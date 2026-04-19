from collections import defaultdict

from fip.ingestion.base import Source
from fip.lakehouse.bronze.base import SinkFactory


def ingest_source_to_sink(source: Source, sink_factory: SinkFactory) -> int:
    """Ingest all records from a source, grouping by entity type before writing.

    Grouping by entity allows specialized sink handling per entity type
    and reduces catalog lookups in storage systems like Iceberg.
    """
    grouped_records = defaultdict(list)

    for record in source.iter_records():
        grouped_records[record.entity_name].append(record)

    written = 0
    for entity_name, records in grouped_records.items():
        sink = sink_factory.for_entity(entity_name)
        written += sink.write(records)

    return written
