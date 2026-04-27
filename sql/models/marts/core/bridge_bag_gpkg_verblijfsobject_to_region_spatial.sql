{{ config(materialized='view') }}

with exact_candidates as (
    select
        v.bag_id as bag_object_id,
        'bag_gpkg_verblijfsobject' as bag_object_type,
        r.region_id,
        'postgis_covers' as mapping_method,
        1.0::numeric as confidence,
        v.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by v.bag_id
        ) as match_count,
        row_number() over (
            partition by v.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_gpkg_verblijfsobject_spatial') }} as v
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and v.geom is not null
        and ST_Covers(r.geom, v.geom)
),
fallback_candidates as (
    select
        v.bag_id as bag_object_id,
        'bag_gpkg_verblijfsobject' as bag_object_type,
        r.region_id,
        'postgis_intersects' as mapping_method,
        0.95::numeric as confidence,
        v.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by v.bag_id
        ) as match_count,
        row_number() over (
            partition by v.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_gpkg_verblijfsobject_spatial') }} as v
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and v.geom is not null
        and ST_Intersects(r.geom, v.geom)
    where not exists (
        select 1
        from {{ ref('stg_region_geom') }} as exact_r
        where exact_r.region_level = 'municipality'
            and v.geom is not null
            and ST_Covers(exact_r.geom, v.geom)
    )
),
all_candidates as (
    select * from exact_candidates
    union all
    select * from fallback_candidates
),
ranked as (
    select
        bag_object_id,
        bag_object_type,
        region_id,
        mapping_method,
        confidence,
        active_from,
        active_to,
        match_count,
        row_number() over (
            partition by bag_object_id, bag_object_type
            order by confidence desc, region_id
        ) as selection_rank
    from all_candidates
)

select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
from ranked
where selection_rank = 1
