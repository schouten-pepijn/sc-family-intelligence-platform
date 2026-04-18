select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    observation_id,
    measure_code,
    period_code,
    region_code,
    numeric_value,
    value_attribute,
    string_value
from {{ ref('stg_cbs_observations') }}
