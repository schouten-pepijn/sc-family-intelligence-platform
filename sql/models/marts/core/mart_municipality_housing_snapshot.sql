{{ config(materialized='table') }}

{# First compact housing snapshot:
   one row per municipality with BAG context and the latest known average
   purchase price from CBS table 83625NED (measure M001534). #}
with municipality_base as (
    select *
    from {{ ref('mart_municipality_overview') }}
),

housing_price_latest as (
    select
        region_id,
        period_year as avg_koopwoningprijs_period_year,
        period_id as avg_koopwoningprijs_period_id,
        measure_id as avg_koopwoningprijs_measure_id,
        measure_title as avg_koopwoningprijs_measure_title,
        average_observation_value as avg_koopwoningprijs,
        row_number() over (
            partition by region_id
            order by period_year desc, period_id desc
        ) as rn
    from {{ ref('mart_cbs_observation_values_by_region_year') }}
    where region_id like 'GM%'
        and measure_id = 'M001534'
),

housing_price as (
    select *
    from housing_price_latest
    where rn = 1
)

select
    base.region_id,
    base.region_title,
    base.region_description,
    base.bag_address_count,
    base.bag_verblijfsobject_count,
    base.bag_pand_count,
    base.mapped_address_count,
    base.bag_adres_mapped_count,
    base.locatieserver_mapped_count,
    base.latest_period_year,
    base.latest_measure_count,
    housing.avg_koopwoningprijs_period_year,
    housing.avg_koopwoningprijs_period_id,
    housing.avg_koopwoningprijs_measure_id,
    housing.avg_koopwoningprijs_measure_title,
    housing.avg_koopwoningprijs
from municipality_base as base
left join housing_price as housing
    on housing.region_id = base.region_id
