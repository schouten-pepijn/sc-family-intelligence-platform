{{ config(materialized='view') }}

{# Spatial BAG->region bridge built from municipal polygons in stg_region_geom.
   This keeps the public bridge name stable while shifting the implementation. #}
with address_exact_candidates as (
    select
        a.bag_id as bag_object_id,
        'bag_adres' as bag_object_type,
        r.region_id,
        'postgis_contains' as mapping_method,
        1.0::numeric as confidence,
        a.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by a.bag_id
        ) as match_count,
        row_number() over (
            partition by a.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_adressen_spatial') }} as a
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and a.geom is not null
        and ST_Contains(r.geom, a.geom)
),
address_fallback_candidates as (
    select
        a.bag_id as bag_object_id,
        'bag_adres' as bag_object_type,
        r.region_id,
        'postgis_intersects' as mapping_method,
        0.95::numeric as confidence,
        a.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by a.bag_id
        ) as match_count,
        row_number() over (
            partition by a.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_adressen_spatial') }} as a
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and a.geom is not null
        and ST_Intersects(r.geom, a.geom)
    where not exists (
        select 1
        from {{ ref('stg_region_geom') }} as exact_r
        where exact_r.region_level = 'municipality'
            and a.geom is not null
            and ST_Contains(exact_r.geom, a.geom)
    )
),
pand_exact_candidates as (
    select
        p.bag_id as bag_object_id,
        'bag_pand' as bag_object_type,
        r.region_id,
        'postgis_contains' as mapping_method,
        1.0::numeric as confidence,
        p.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by p.bag_id
        ) as match_count,
        row_number() over (
            partition by p.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_pand_spatial') }} as p
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and p.geom is not null
        and ST_Contains(r.geom, p.geom)
),
pand_fallback_candidates as (
    select
        p.bag_id as bag_object_id,
        'bag_pand' as bag_object_type,
        r.region_id,
        'postgis_intersects' as mapping_method,
        0.95::numeric as confidence,
        p.retrieved_at::date as active_from,
        null::date as active_to,
        count(*) over (
            partition by p.bag_id
        ) as match_count,
        row_number() over (
            partition by p.bag_id
            order by r.region_id
        ) as row_number
    from {{ ref('stg_bag_pand_spatial') }} as p
    join {{ ref('stg_region_geom') }} as r
        on r.region_level = 'municipality'
        and p.geom is not null
        and ST_Intersects(r.geom, p.geom)
    where not exists (
        select 1
        from {{ ref('stg_region_geom') }} as exact_r
        where exact_r.region_level = 'municipality'
            and p.geom is not null
            and ST_Contains(exact_r.geom, p.geom)
    )
),
all_candidates as (
    select * from address_exact_candidates
    union all
    select * from address_fallback_candidates
    union all
    select * from pand_exact_candidates
    union all
    select * from pand_fallback_candidates
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
