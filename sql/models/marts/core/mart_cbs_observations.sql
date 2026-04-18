with staged as (
    select *
    from {{ ref('stg_cbs_observations') }}
)

select
    source_name,
    run_id,
    retrieved_at,
    natural_key as observation_key,
    observation_id,
    measure_code as measure_id,
    period_code as period_id,
    case
        when period_code ~ '^[0-9]{4}' then cast(substring(period_code from 1 for 4) as integer)
        else null
    end as period_year,
    case
        when substring(period_code from 5 for 2) = 'JJ' then 'year'
        when substring(period_code from 5 for 2) = 'KW' then 'quarter'
        when substring(period_code from 5 for 2) = 'MM' then 'month'
        else 'other'
    end as period_granularity,
    region_code as region_id,
    numeric_value as observation_value,
    value_attribute,
    string_value
from staged
