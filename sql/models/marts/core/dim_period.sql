{{ config(materialized='view') }}

select
    period_id,
    identifier,
    period_title,
    period_description,
    dimension_group_id,
    status,
    period_year,
    period_granularity
from {{ ref('stg_cbs_period_codes') }}
