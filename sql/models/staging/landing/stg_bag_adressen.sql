{{ config(materialized='view') }}

{# Normalize the BAG adres landing table before marts consume it. #}
select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    bag_id,
    adres_identificatie,
    postcode,
    huisnummer,
    huisletter,
    toevoeging,
    openbare_ruimte_naam,
    woonplaats_naam,
    bronhouder_identificatie,
    gemeentecode,
    gemeentenaam,
    geometry
from {{ source('landing', 'bag_adressen') }}
