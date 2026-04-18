{{ config(materialized='view') }}

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
from {{ source('landing', 'cbs_observations') }}
