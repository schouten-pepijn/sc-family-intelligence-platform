{{ config(materialized='view') }}

{# Conformed geo dimension based on CBS region codes.
   Keep this stable and source-driven; BAG mapping comes later via a bridge. #}
select
    region_id,
    identifier,
    region_title,
    region_description,
    dimension_group_id
from {{ ref('stg_cbs_region_codes') }}
