{{ config(materialized='view') }}

{# Bridge between BAG objects and the conformed geo region dimension.
   Seed-driven for now so the mapping is explicit and auditable. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
from {{ ref('bag_geo_region_mapping') }}
