{{ config(materialized='view') }}

select
    natural_key as measure_id,
    identifier,
    title as measure_title,
    description as measure_description,
    measure_group_id,
    data_type,
    unit,
    decimals,
    presentation_type,
    source_name,
    retrieved_at,
    run_id,
    schema_version,
    http_status
from {{ source('landing', 'cbs_measure_codes') }}
