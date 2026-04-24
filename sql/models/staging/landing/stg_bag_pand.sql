{{ config(materialized='view') }}

{# Normalize the BAG pand landing table before marts consume it. #}
select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    bag_id,
    pand_identificatie,
    pand_status,
    oorspronkelijk_bouwjaar,
    geconstateerd,
    documentdatum,
    documentnummer,
    geometry
from {{ source('landing', 'bag_pand') }}
