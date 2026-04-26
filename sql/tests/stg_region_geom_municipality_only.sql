-- fails if any region_geom row is not a municipality-level geometry
select *
from {{ ref('stg_region_geom') }}
where region_level <> 'municipality'
