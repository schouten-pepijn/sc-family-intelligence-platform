{{ config(materialized='view') }}

{# Periods dimension maps CBS period codes to human-readable labels and includes
   derived fields (year, granularity, status) to support temporal analysis. #}
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
