{{ config(materialized='view') }}

{# Staging view over the CBS 85035NED observations landing table.
   This preserves the housing-type dimensions needed to model stock and surface
   metrics without collapsing the table into a generic region/year aggregate. #}
select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    observation_id,
    measure_code,
    period_code,
    region_code,
    numeric_value,
    value_attribute,
    string_value,
    woningtype_code,
    woningkenmerk_code
from {{ source('landing', 'cbs_observations_85035ned') }}
