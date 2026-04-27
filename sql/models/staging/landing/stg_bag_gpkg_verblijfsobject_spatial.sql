{{ config(
    materialized='table',
    post_hook=[
        "create index if not exists idx_{{ this.name }}_geom_gist on {{ this }} using gist (geom)"
    ]
) }}

select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    bag_id,
    verblijfsobject_identificatie,
    hoofdadres_identificatie,
    geometry as geometry_json,
    case
        when geometry is null then null
        else ST_Transform(
            ST_SetSRID(ST_GeomFromGeoJSON(geometry), {{ var('bag_gpkg_source_srid', 28992) }}),
            4326
        )
    end as geom
from {{ ref('stg_bag_gpkg_verblijfsobject') }}
