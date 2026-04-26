-- fails if any non-null geom is invalid
select *
from {{ ref('stg_region_geom') }}
where geom is not null
  and not st_isvalid(geom)
