{{ config(materialized='view') }}

{# Normalize the BAG GeoPackage verblijfsobject landing table for the v2 flow. #}
select
    source_name,
    natural_key,
    retrieved_at,
    run_id,
    schema_version,
    http_status,
    bag_id,
    verblijfsobject_identificatie,
    hoofdadres_identificatie,
    postcode,
    huisnummer,
    huisletter,
    toevoeging,
    woonplaats_naam,
    openbare_ruimte_naam,
    gebruiksdoel,
    oppervlakte,
    geometry
from {{ source('landing', 'bag_gpkg_verblijfsobject') }}
