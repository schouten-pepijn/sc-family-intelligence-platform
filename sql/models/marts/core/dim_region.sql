{{ config(materialized='view') }}

select
    region_id,
    identifier,
    region_title,
    region_description,
    dimension_group_id
from {{ ref('stg_cbs_region_codes') }}
