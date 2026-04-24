{{ config(materialized='table') }}

{# First consumable cross-source mart:
   CBS municipality/region statistics with BAG address coverage. #}
with cbs_scores as (
    select *
    from {{ ref('mart_cbs_observation_values_by_region_year') }}
    where region_id like 'GM%'
),
regions as (
    select *
    from {{ ref('dim_region') }}
),
bag_coverage as (
    select *
    from {{ ref('mart_bag_region_coverage') }}
)
select
    concat_ws('|', cbs_scores.region_id, cbs_scores.period_id, cbs_scores.measure_id) as scorecard_key,
    cbs_scores.region_id,
    coalesce(cbs_scores.region_title, regions.region_title) as region_title,
    regions.region_description,
    cbs_scores.region_dimension_group_id,
    cbs_scores.period_year,
    cbs_scores.period_id,
    cbs_scores.period_title,
    cbs_scores.period_granularity,
    cbs_scores.period_status,
    cbs_scores.measure_id,
    cbs_scores.measure_title,
    cbs_scores.measure_description,
    cbs_scores.data_type,
    cbs_scores.unit,
    cbs_scores.presentation_type,
    cbs_scores.observation_count,
    cbs_scores.average_observation_value,
    cbs_scores.min_observation_value,
    cbs_scores.max_observation_value,
    coalesce(bag_coverage.mapped_address_count, 0) as mapped_address_count,
    coalesce(bag_coverage.bag_adres_mapped_count, 0) as bag_adres_mapped_count,
    coalesce(bag_coverage.locatieserver_mapped_count, 0) as locatieserver_mapped_count,
    bag_coverage.first_mapped_at,
    bag_coverage.last_mapped_at
from cbs_scores
left join regions
    on cbs_scores.region_id = regions.region_id
left join bag_coverage
    on cbs_scores.region_id = bag_coverage.region_id
