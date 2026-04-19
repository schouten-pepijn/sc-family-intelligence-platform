{{ config(materialized='view') }}

{# Regions dimension maps CBS region codes (provinces, municipalities) to readable names
   and dimension groups for hierarchical filtering in downstream applications. #}
select
    region_id,
    identifier,
    region_title,
    region_description,
    dimension_group_id
from {{ ref('stg_cbs_region_codes') }}
