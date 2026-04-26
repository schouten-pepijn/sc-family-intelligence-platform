-- fails if any non-null geom has SRID != 4326
select *
from {{ ref('stg_bag_adressen_spatial') }}
where geom is not null
  and st_srid(geom) <> 4326
