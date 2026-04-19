{{ config(materialized='view') }}

select
    natural_key as period_id,
    identifier,
    title as period_title,
    description as period_description,
    dimension_group_id,
    status,
    case
        when identifier ~ '^[0-9]{4}'
            then cast(substring(identifier from 1 for 4) as integer)
        else null
    end as period_year,
    case
        when substring(identifier from 5 for 2) = 'JJ' then 'year'
        when substring(identifier from 5 for 2) = 'KW' then 'quarter'
        when substring(identifier from 5 for 2) = 'MM' then 'month'
        else 'other'
    end as period_granularity,
    source_name,
    retrieved_at,
    run_id,
    schema_version,
    http_status
from {{ source('landing', 'cbs_period_codes') }}
