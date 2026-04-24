{{ config(materialized='view') }}

{# Eigendom dimension exposes the labels behind the CBS 85036NED eigendom
   codes so marts can read human-friendly categories instead of raw codes. #}
select
    eigendom_id,
    identifier,
    eigendom_title,
    eigendom_description,
    dimension_group_id
from {{ ref('stg_cbs_eigendom_codes') }}
