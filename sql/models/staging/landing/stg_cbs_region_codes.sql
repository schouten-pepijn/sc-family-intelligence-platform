{{ config(materialized='view') }}

{# Renames source fields to match domain terminology; dimension_group_id supports
   hierarchical filtering (e.g., by provincial/municipal groupings). #}
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
