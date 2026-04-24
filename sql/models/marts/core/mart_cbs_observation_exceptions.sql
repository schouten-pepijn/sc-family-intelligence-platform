{{ config(materialized='view') }}

{# Debug / QA mart for CBS observations that fail dimensional enrichment.
   This preserves the orphaned rows that are filtered out of the canonical CBS
   marts so they can be inspected without weakening the main contracts. #}
with staged as (
    select *
    from {{ ref('stg_cbs_observations_83625') }}
),
enriched as (
    select
        staged.source_name,
        staged.run_id,
        staged.retrieved_at,
        staged.natural_key as observation_key,
        staged.observation_id,
        staged.measure_code as measure_id,
        measure.measure_title,
        measure.measure_description,
        staged.period_code as period_id,
        period.period_title,
        period.period_description,
        period.period_year,
        period.period_granularity,
        period.status as period_status,
        staged.region_code as region_id,
        region.region_title,
        region.region_description,
        region.dimension_group_id as region_dimension_group_id,
        staged.numeric_value as observation_value,
        staged.value_attribute,
        staged.string_value,
        case when measure.measure_id is null then true else false end as missing_measure,
        case when period.period_id is null then true else false end as missing_period,
        case when region.region_id is null then true else false end as missing_region
    from staged
    left join {{ ref('dim_measure') }} as measure
        on staged.measure_code = measure.measure_id
    left join {{ ref('dim_period') }} as period
        on staged.period_code = period.period_id
    left join {{ ref('dim_region') }} as region
        on staged.region_code = region.region_id
)

select *
from enriched
where missing_measure
    or missing_period
    or missing_region
