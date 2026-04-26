{{ config(materialized='table') }}

with base_addresses as (
    select
        bag_id as bag_object_id,
        adres_identificatie,
        postcode,
        gemeentecode as bag_gemeentecode,
        gemeentenaam as bag_gemeentenaam,
        geom
    from {{ ref('stg_bag_adressen_spatial') }}
),
legacy_candidates as (
    select
        bag_object_id,
        bag_object_type,
        region_id as legacy_region_id,
        mapping_method as legacy_mapping_method,
        count(*) over (
            partition by bag_object_id, bag_object_type
        ) as legacy_match_count,
        row_number() over (
            partition by bag_object_id, bag_object_type
            order by active_from desc nulls last, active_to desc nulls last, region_id
        ) as legacy_row_number
    from {{ ref('bridge_bag_to_geo_region_legacy_code') }}
    where bag_object_type = 'bag_adres'
),
legacy as (
    select
        bag_object_id,
        bag_object_type,
        legacy_region_id,
        legacy_mapping_method,
        legacy_match_count
    from legacy_candidates
    where legacy_row_number = 1
),
spatial_candidates as (
    select
        a.bag_object_id,
        'bag_adres' as bag_object_type,
        r.region_id as spatial_region_id,
        count(*) over (partition by a.bag_object_id) as spatial_match_count,
        row_number() over (
            partition by a.bag_object_id
            order by r.region_id
        ) as spatial_row_number
    from base_addresses as a
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and a.geom is not null
        and ST_Covers(r.geom, a.geom)
),
spatial as (
    select
        bag_object_id,
        bag_object_type,
        spatial_region_id,
        spatial_match_count
    from spatial_candidates
    where spatial_row_number = 1
),
regions as (
    select
        region_id,
        region_title
    from {{ ref('dim_region') }}
)

select
    a.bag_object_id,
    'bag_adres' as bag_object_type,
    a.adres_identificatie,
    a.postcode,
    a.bag_gemeentecode,
    a.bag_gemeentenaam,
    l.legacy_region_id,
    lr.region_title as legacy_region_title,
    l.legacy_mapping_method,
    coalesce(l.legacy_match_count, 0) as legacy_match_count,
    s.spatial_region_id,
    sr.region_title as spatial_region_title,
    coalesce(s.spatial_match_count, 0) as spatial_match_count,
    case
        when coalesce(l.legacy_match_count, 0) > 1 and coalesce(s.spatial_match_count, 0) > 1
            then 'both_ambiguous'
        when coalesce(l.legacy_match_count, 0) > 1
            then 'legacy_ambiguous'
        when coalesce(s.spatial_match_count, 0) > 1
            then 'spatial_ambiguous'
        when l.legacy_region_id is not null and s.spatial_region_id is not null and l.legacy_region_id = s.spatial_region_id
            then 'match'
        when l.legacy_region_id is not null and s.spatial_region_id is not null
            then 'mismatch'
        when l.legacy_region_id is not null and s.spatial_region_id is null
            then 'legacy_only'
        when l.legacy_region_id is null and s.spatial_region_id is not null
            then 'spatial_only'
        else 'no_match'
    end as comparison_status
from base_addresses as a
left join legacy as l
    on l.bag_object_id = a.bag_object_id
   and l.bag_object_type = 'bag_adres'
left join spatial as s
    on s.bag_object_id = a.bag_object_id
   and s.bag_object_type = 'bag_adres'
left join regions as lr
    on lr.region_id = l.legacy_region_id
left join regions as sr
    on sr.region_id = s.spatial_region_id
