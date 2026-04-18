select
    region_id,
    period_year,
    measure_id,
    count(*) as observation_count,
    avg(observation_value) as average_observation_value,
    min(observation_value) as min_observation_value,
    max(observation_value) as max_observation_value
from {{ ref('mart_cbs_observations') }}
where observation_value is not null
    and period_year is not null
group by
    region_id,
    period_year,
    measure_id
