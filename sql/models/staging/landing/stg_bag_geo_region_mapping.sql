{{ config(materialized='view') }}

{# Normalize BAG geo-region mappings before the bridge and marts consume them. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to,
    run_id
from {{ source('landing', 'bag_geo_region_mapping') }}
