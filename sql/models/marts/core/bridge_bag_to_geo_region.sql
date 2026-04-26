{{ config(materialized='view') }}

{# Public BAG->region bridge. This now points at the spatial implementation
   while keeping the stable downstream model name. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
from {{ ref('bridge_bag_to_geo_region_spatial') }}
