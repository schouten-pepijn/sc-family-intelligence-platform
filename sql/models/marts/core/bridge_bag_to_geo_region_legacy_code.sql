{{ config(materialized='view') }}

{# Legacy BAG->region bridge backed by the bootstrap seed. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
from {{ ref('stg_bag_geo_region_mapping') }}
