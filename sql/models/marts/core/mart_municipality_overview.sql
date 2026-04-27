{{ config(materialized='table') }}

{# First consumable cross-source mart:
   CBS municipality/region statistics with BAG object coverage. #}
with regions as (
    select
        region_id,
        region_title,
        region_description
    from {{ ref('dim_region') }}
    where region_id like 'GM%'
),

{% if var('bag_region_bridge_source', 'gpkg_v2') == 'gpkg_v2' %}
bag_address_counts as (
    select
        region_id,
        0::bigint as bag_address_count
    from regions
),

bag_verblijfsobject_counts as (
    select
        region_id,
        count(distinct bag_object_id) as bag_verblijfsobject_count
    from {{ ref('bridge_bag_to_geo_region') }}
    where bag_object_type = 'bag_gpkg_verblijfsobject'
    group by region_id
),

bag_pand_counts as (
    select
        region_id,
        0::bigint as bag_pand_count
    from regions
),
{% elif var('bag_region_bridge_source', 'gpkg_v2') == 'legacy_spatial' %}
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
    select
        b.region_id,
        count(distinct p.bag_id) as bag_pand_count
    from {{ ref('stg_bag_pand') }} as p
    inner join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = p.bag_id
        and b.bag_object_type = 'bag_pand'
    group by b.region_id
),
{% else %}
    {{ exceptions.raise_compiler_error(
        "Unsupported bag_region_bridge_source var. Expected 'gpkg_v2' or 'legacy_spatial'."
    ) }}
{% endif %}

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
