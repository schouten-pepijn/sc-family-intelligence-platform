-- fails if any non-null geom is invalid
select *
from {{ ref('stg_bag_adressen_spatial') }}
where geom is not null
  and not st_isvalid(geom)
