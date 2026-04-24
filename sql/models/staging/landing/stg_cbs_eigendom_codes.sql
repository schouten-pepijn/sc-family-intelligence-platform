{{ config(materialized='view') }}

{# Eigendom codes expose the category labels behind the 85036NED observations.
   The raw observation rows carry coded Eigendom values; this staging view
   normalizes the landing table into a reusable dimension input. #}
select
    natural_key as eigendom_id,
    identifier,
    title as eigendom_title,
    description as eigendom_description,
    dimension_group_id,
    source_name,
    retrieved_at,
    run_id,
    schema_version,
    http_status
from {{ source('landing', 'cbs_eigendom_codes') }}
