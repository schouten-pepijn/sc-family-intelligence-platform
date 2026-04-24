{{ config(materialized='table') }}

{# Municipality WOZ snapshot built from CBS 85036NED.
   This keeps the municipality grain and uses the total eigendom slice
   (T001132) as the first stable WOZ view. #}
with staged as (
    select *
    from {{ ref('stg_cbs_observations_85036') }}
),
koopwoning_candidates as (
    select
        region_id,
        period_year as koopwoningprijs_period_year,
        period_id as koopwoningprijs_period_id,
        measure_id as koopwoningprijs_measure_id,
        measure_title as koopwoningprijs_measure_title,
        average_observation_value as koopwoningprijs,
        row_number() over (
            partition by region_id
            order by period_year desc, period_id desc
        ) as rn
    from {{ ref('mart_cbs_observation_values_by_region_year') }}
    where region_id like 'GM%'
        and measure_id = 'M001534'
),
koopwoning_latest as (
    select *
    from koopwoning_candidates
    where rn = 1
),

woz_candidates as (
    select
        staged.region_code as region_id,
        region.region_title,
        region.region_description,
        period.period_year,
        staged.period_code as period_id,
        period.period_title,
        staged.measure_code as measure_id,
        coalesce(measure.measure_title, 'Gemiddelde WOZ-waarde van woningen') as measure_title,
        staged.eigendom_code as eigendom_id,
        eigendom.eigendom_title,
        staged.numeric_value as avg_woz_value_thousands,
        staged.numeric_value as observation_value,
        row_number() over (
            partition by staged.region_code
            order by period.period_year desc, staged.period_code desc
        ) as rn
    from staged
    left join {{ ref('dim_measure') }} as measure
        on staged.measure_code = measure.measure_id
    left join {{ ref('dim_eigendom') }} as eigendom
        on staged.eigendom_code = eigendom.eigendom_id
    left join {{ ref('dim_period') }} as period
        on staged.period_code = period.period_id
    left join {{ ref('dim_region') }} as region
        on staged.region_code = region.region_id
    where staged.region_code like 'GM%'
        and staged.measure_code = 'M003039'
        and staged.eigendom_code = 'T001132'
        and staged.numeric_value is not null
        and period.period_year is not null
),

woz_latest as (
    select *
    from woz_candidates
    where rn = 1
)

select
    woz_latest.region_id,
    woz_latest.region_title,
    woz_latest.region_description,
    woz_latest.period_year as woz_period_year,
    woz_latest.period_id as woz_period_id,
    woz_latest.period_title as woz_period_title,
    woz_latest.measure_id as woz_measure_id,
    woz_latest.measure_title as woz_measure_title,
    woz_latest.eigendom_id,
    woz_latest.eigendom_title,
    woz_latest.avg_woz_value_thousands,
    woz_latest.observation_value * 1000 as avg_woz_value,
    koopwoning.koopwoningprijs_period_year,
    koopwoning.koopwoningprijs_period_id,
    koopwoning.koopwoningprijs_measure_id,
    koopwoning.koopwoningprijs_measure_title,
    koopwoning.koopwoningprijs
from woz_latest
left join koopwoning_latest as koopwoning
    on koopwoning.region_id = woz_latest.region_id
