{{ config(materialized='view') }}

{# Bridge between BAG objects and the conformed geo region dimension.
   Runtime input comes from the staging layer; the seed is only a legacy bootstrap fixture. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
from {{ ref('stg_bag_geo_region_mapping') }}
