{{ config(materialized='view') }}

select
    natural_key as region_id,
    identifier,
    title as region_title,
    description as region_description,
    dimension_group_id,
    source_name,
    retrieved_at,
    run_id,
    schema_version,
    http_status
from {{ source('landing', 'cbs_region_codes') }}
