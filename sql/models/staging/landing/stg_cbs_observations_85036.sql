{{ config(materialized='view') }}

{# Staging layer for the CBS 85036NED observations landing table.
   This mirrors the generic CBS observations staging view but keeps the 85036
   source isolated so it can be modelled separately from the existing 83625 feed. #}
select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    observation_id,
    measure_code,
    period_code,
    region_code,
    numeric_value,
    value_attribute,
    string_value
from {{ source('landing', 'cbs_observations_85036ned') }}
