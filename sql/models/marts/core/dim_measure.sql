{{ config(materialized='view') }}

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
