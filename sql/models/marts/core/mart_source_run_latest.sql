{{ config(materialized='view') }}

{# Latest archived run per source_name, used as the first operational consumer of source run manifests. #}
with ranked_runs as (
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
        file_size_bytes,
        row_number() over (
            partition by source_name
            order by started_at desc, finished_at desc, run_id desc
        ) as rn
    from {{ ref('dim_source_run') }}
)

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
from ranked_runs
where rn = 1
