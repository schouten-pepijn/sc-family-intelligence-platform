{{ config(materialized='view') }}

select
    source_name,
    source_family,
    run_id,
    started_at,
    finished_at,
    source_url,
    source_version,
    license,
    attribution,
    raw_uri,
    row_count,
    status,
    error_message,
    checksum,
    file_size_bytes
from {{ source('landing', 'source_runs') }}
