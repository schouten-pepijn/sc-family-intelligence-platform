{{ config(materialized='table') }}

{# First compact housing snapshot:
   one row per municipality with BAG context from the spatial bridge and the
   latest known average purchase price from CBS table 83625NED (measure
   M001534), plus the latest housing-stock and average-surface signals from
   CBS table 85035NED. #}
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
),

housing_85035_candidates as (
    select
        staged.region_code as region_id,
        region.region_title,
        region.region_description,
        period.period_year,
        staged.period_code as period_id,
        period.period_title,
        staged.measure_code as measure_id,
        coalesce(measure.measure_title, staged.measure_code) as measure_title,
        staged.numeric_value,
        row_number() over (
            partition by staged.region_code, staged.measure_code
            order by period.period_year desc, staged.period_code desc
        ) as rn
    from {{ ref('stg_cbs_observations_85035') }} as staged
    left join {{ ref('dim_measure') }} as measure
        on staged.measure_code = measure.measure_id
    left join {{ ref('dim_period') }} as period
        on staged.period_code = period.period_id
    left join {{ ref('dim_region') }} as region
        on staged.region_code = region.region_id
    where staged.region_code like 'GM%'
        and staged.woningtype_code = 'Totaal woningen'
        and staged.woningkenmerk_code = 'Totaal woningen'
        and staged.numeric_value is not null
        and period.period_year is not null
        and measure.measure_title in (
            'Beginstand woningvoorraad (aantal)',
            'Gemiddelde oppervlakte (m2)'
        )
),

housing_85035 as (
    select
        region_id,
        max(case when measure_title = 'Beginstand woningvoorraad (aantal)' then period_year end) as housing_stock_period_year,
        max(case when measure_title = 'Beginstand woningvoorraad (aantal)' then period_id end) as housing_stock_period_id,
        max(case when measure_title = 'Beginstand woningvoorraad (aantal)' then measure_id end) as housing_stock_measure_id,
        max(case when measure_title = 'Beginstand woningvoorraad (aantal)' then measure_title end) as housing_stock_measure_title,
        max(case when measure_title = 'Beginstand woningvoorraad (aantal)' then numeric_value end) as housing_stock,
        max(case when measure_title = 'Gemiddelde oppervlakte (m2)' then period_year end) as avg_housing_surface_period_year,
        max(case when measure_title = 'Gemiddelde oppervlakte (m2)' then period_id end) as avg_housing_surface_period_id,
        max(case when measure_title = 'Gemiddelde oppervlakte (m2)' then measure_id end) as avg_housing_surface_measure_id,
        max(case when measure_title = 'Gemiddelde oppervlakte (m2)' then measure_title end) as avg_housing_surface_measure_title,
        max(case when measure_title = 'Gemiddelde oppervlakte (m2)' then numeric_value end) as avg_housing_surface_m2
    from housing_85035_candidates
    where rn = 1
    group by region_id
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
    housing_85035.housing_stock_period_year,
    housing_85035.housing_stock_period_id,
    housing_85035.housing_stock_measure_id,
    housing_85035.housing_stock_measure_title,
    housing_85035.housing_stock,
    housing_85035.avg_housing_surface_period_year,
    housing_85035.avg_housing_surface_period_id,
    housing_85035.avg_housing_surface_measure_id,
    housing_85035.avg_housing_surface_measure_title,
    housing_85035.avg_housing_surface_m2,
    housing.avg_koopwoningprijs_period_year,
    housing.avg_koopwoningprijs_period_id,
    housing.avg_koopwoningprijs_measure_id,
    housing.avg_koopwoningprijs_measure_title,
    housing.avg_koopwoningprijs
from municipality_base as base
left join housing_price as housing
    on housing.region_id = base.region_id
left join housing_85035 as housing_85035
    on housing_85035.region_id = base.region_id
