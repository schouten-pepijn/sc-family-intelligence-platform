-- fails if any region_id does not exist in the conformed region dimension
select r.*
from {{ ref('stg_region_geom') }} as r
left join {{ ref('dim_region') }} as d
  on r.region_id = d.region_id
where d.region_id is null
