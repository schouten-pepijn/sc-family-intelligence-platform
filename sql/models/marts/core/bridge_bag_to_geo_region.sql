{{ config(materialized='view') }}

{# Public BAG->region bridge.
   Default source is the BAG GeoPackage v2 bridge. Set
   --vars '{"bag_region_bridge_source": "legacy_spatial"}' to roll back to the
   legacy OGC spatial bridge without changing downstream model names. #}
select
    bag_object_id,
    bag_object_type,
    region_id,
    mapping_method,
    confidence,
    active_from,
    active_to
{% if var('bag_region_bridge_source', 'gpkg_v2') == 'legacy_spatial' %}
from {{ ref('bridge_bag_to_geo_region_spatial') }}
{% elif var('bag_region_bridge_source', 'gpkg_v2') == 'gpkg_v2' %}
from {{ ref('bridge_bag_gpkg_verblijfsobject_to_region_spatial') }}
{% else %}
    {{ exceptions.raise_compiler_error(
        "Unsupported bag_region_bridge_source var. Expected 'gpkg_v2' or 'legacy_spatial'."
    ) }}
{% endif %}
