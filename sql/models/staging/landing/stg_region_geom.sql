{{ config(
    materialized='table',
    post_hook=[
        "create index if not exists idx_{{ this.name }}_geom_gist on {{ this }} using gist (geom)",
        "create index if not exists idx_{{ this.name }}_region_id on {{ this }} (region_id)"
    ]
) }}

with source as (
    select
        source_name,
        natural_key,
        retrieved_at,
        run_id,
        schema_version,
        http_status,
        region_id,
        region_level,
        region_name,
        source_version,
        valid_from,
        geometry as geometry_json
    from {{ source('landing', 'region_geom') }}
)

select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    region_id,
    region_level,
    region_name,
    source_version,
    valid_from,
    geometry_json,
    case
        when geometry_json is null then null
        else ST_Transform(ST_SetSRID(ST_GeomFromGeoJSON(geometry_json), {{ var('region_source_srid', 28992) }}), 4326)
    end as geom
from source
