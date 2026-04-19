{{ config(materialized='view') }}

{# Measures dimension provides code mappings and metadata from the CBS data source.
   Materialized as a view since codes are static and sourced independently. #}
select
    measure_id,
    identifier,
    measure_title,
    measure_description,
    measure_group_id,
    data_type,
    unit,
    decimals,
    presentation_type
from {{ ref('stg_cbs_measure_codes') }}
