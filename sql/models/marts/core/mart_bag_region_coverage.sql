{{ config(materialized='view') }}

{# First cross-source mart: spatial BAG mapped addresses summarized by CBS region.
   This combines the public BAG->region bridge with the conformed CBS region dimension. #}
with bag_region_mapping as (
    select *
    from {{ ref('bridge_bag_to_geo_region') }}
    where bag_object_type = 'bag_adres'
),
regions as (
    select *
    from {{ ref('dim_region') }}
)
select
    bag_region_mapping.region_id,
    regions.region_title,
    regions.region_description,
    regions.dimension_group_id as region_dimension_group_id,
    count(*) as mapped_address_count,
    count(*) as bag_adres_mapped_count,
    0::bigint as locatieserver_mapped_count,
    min(bag_region_mapping.active_from) as first_mapped_at,
    max(bag_region_mapping.active_from) as last_mapped_at
from bag_region_mapping
inner join regions
    on bag_region_mapping.region_id = regions.region_id
group by
    bag_region_mapping.region_id,
    regions.region_title,
    regions.region_description,
    regions.dimension_group_id
