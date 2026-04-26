{{ config(materialized='table') }}

{# Municipality-level BAG data-quality mart.
   This uses the spatial BAG municipality bridge and makes coverage and
   missing-field problems visible per GMxxxx region. #}
with regions as (
    select
        region_id,
        region_title,
        region_description
    from {{ ref('dim_region') }}
    where region_id like 'GM%'
),
address_quality as (
    select
        coalesce(a.gemeentecode, b.region_id) as region_id,
        count(distinct a.bag_id) as bag_address_count,
        count(*) filter (
            where nullif(trim(coalesce(a.postcode, '')), '') is null
        ) as missing_postcode_count,
        count(*) filter (
            where a.huisnummer is null
        ) as missing_huisnummer_count,
        count(*) filter (where a.geometry is null) as missing_geometry_count,
        count(*) filter (
            where nullif(trim(coalesce(a.bronhouder_identificatie, '')), '') is null
        ) as missing_bronhouder_identificatie_count
    from {{ ref('stg_bag_adressen') }} as a
    left join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = a.bag_id
        and b.bag_object_type = 'bag_adres'
    group by coalesce(a.gemeentecode, b.region_id)
),
verblijfsobject_counts as (
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
pand_counts as (
    select
        b.region_id,
        count(distinct p.bag_id) as bag_pand_count
    from {{ ref('stg_bag_pand') }} as p
    inner join {{ ref('bridge_bag_to_geo_region') }} as b
        on b.bag_object_id = p.bag_id
        and b.bag_object_type = 'bag_pand'
    group by b.region_id
),
coverage as (
    select
        region_id,
        mapped_address_count,
        bag_adres_mapped_count,
        locatieserver_mapped_count
    from {{ ref('mart_bag_region_coverage') }}
)

select
    r.region_id,
    r.region_title,
    r.region_description,
    coalesce(ad.bag_address_count, 0) as bag_address_count,
    coalesce(vo.bag_verblijfsobject_count, 0) as bag_verblijfsobject_count,
    coalesce(pd.bag_pand_count, 0) as bag_pand_count,
    coalesce(cov.mapped_address_count, 0) as mapped_address_count,
    coalesce(
        coalesce(cov.mapped_address_count, 0)::double precision
        / nullif(coalesce(ad.bag_address_count, 0), 0),
        0.0
    ) as mapping_coverage_ratio,
    coalesce(ad.missing_postcode_count, 0) as missing_postcode_count,
    coalesce(ad.missing_huisnummer_count, 0) as missing_huisnummer_count,
    coalesce(ad.missing_geometry_count, 0) as missing_geometry_count,
    coalesce(ad.missing_bronhouder_identificatie_count, 0) as missing_bronhouder_identificatie_count,
    coalesce(cov.bag_adres_mapped_count, 0) as bag_adres_mapped_count,
    coalesce(cov.locatieserver_mapped_count, 0) as locatieserver_mapped_count
from regions as r
left join address_quality as ad
    on ad.region_id = r.region_id
left join verblijfsobject_counts as vo
    on vo.region_id = r.region_id
left join pand_counts as pd
    on pd.region_id = r.region_id
left join coverage as cov
    on cov.region_id = r.region_id
