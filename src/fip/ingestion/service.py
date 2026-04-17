from fip.ingestion.base import Source
from fip.sink.base import Sink


def ingest_source_to_sink(source: Source, sink: Sink) -> int:
    records = list(source.iter_records())
    return sink.write(records)
