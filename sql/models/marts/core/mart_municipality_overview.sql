{{ config(materialized='table') }}

{# First consumable cross-source mart:
   CBS municipality/region statistics with BAG address coverage.
   The BAG side now comes from the spatial bridge. #}
with regions as (
    select
        region_id,
        region_title,
        region_description
    from {{ ref('dim_region') }}
    where region_id like 'GM%'
),

bag_address_counts as (
    select
        coalesce(a.gemeentecode, b.region_id) as region_id,
        count(distinct a.bag_id) as bag_address_count
    from {{ ref('stg_bag_adressen') }} as a
    left join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = a.bag_id
        and b.bag_object_type = 'bag_adres'
    group by coalesce(a.gemeentecode, b.region_id)
),

bag_verblijfsobject_counts as (
    select
        coalesce(a.gemeentecode, b.region_id) as region_id,
        count(distinct v.bag_id) as bag_verblijfsobject_count
    from {{ ref('stg_bag_verblijfsobject') }} as v
    left join {{ ref('stg_bag_adressen') }} as a
        on a.adres_identificatie = v.hoofdadres_identificatie
    left join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = a.bag_id
        and b.bag_object_type = 'bag_adres'
    group by coalesce(a.gemeentecode, b.region_id)
),

bag_pand_counts as (
    -- Pand counts remain bridge-limited until a spatial or address-based pand
    -- municipality mapping exists. This keeps the mart honest about coverage.
    select
        b.region_id,
        count(distinct p.bag_id) as bag_pand_count
    from {{ ref('stg_bag_pand') }} as p
    inner join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = p.bag_id
        and b.bag_object_type = 'bag_pand'
    group by b.region_id
),

cbs_latest as (
    select
        latest.region_id,
        latest.period_year as latest_period_year,
        coalesce(measures.latest_measure_count, 0) as latest_measure_count
    from (
        select
            region_id,
            max(period_year) as period_year
        from {{ ref('mart_cbs_observation_values_by_region_year') }}
        where region_id like 'GM%'
        group by region_id
    ) as latest
    left join (
        select
            region_id,
            period_year,
            count(distinct measure_id) as latest_measure_count
        from {{ ref('mart_cbs_observation_values_by_region_year') }}
        where region_id like 'GM%'
        group by region_id,
            period_year
    ) as measures
        on measures.region_id = latest.region_id
        and measures.period_year = latest.period_year
)

select
    r.region_id,
    r.region_title,
    r.region_description,
    coalesce(ad.bag_address_count, 0) as bag_address_count,
    coalesce(vo.bag_verblijfsobject_count, 0) as bag_verblijfsobject_count,
    coalesce(pd.bag_pand_count, 0) as bag_pand_count,
    coalesce(mb.mapped_address_count, 0) as mapped_address_count,
    coalesce(mb.bag_adres_mapped_count, 0) as bag_adres_mapped_count,
    coalesce(mb.locatieserver_mapped_count, 0) as locatieserver_mapped_count,
    cl.latest_period_year,
    coalesce(cl.latest_measure_count, 0) as latest_measure_count
from regions as r
left join bag_address_counts as ad
    on ad.region_id = r.region_id
left join bag_verblijfsobject_counts as vo
    on vo.region_id = r.region_id
left join bag_pand_counts as pd
    on pd.region_id = r.region_id
left join {{ ref('mart_bag_region_coverage') }} as mb
    on mb.region_id = r.region_id
left join cbs_latest as cl
    on cl.region_id = r.region_id
