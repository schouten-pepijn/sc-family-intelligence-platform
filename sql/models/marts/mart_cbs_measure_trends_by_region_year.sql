select
    measure_id,
    region_id,
    period_year,
    count(*) as observation_count,
    avg(observation_value) as average_measure_value,
    min(observation_value) as min_measure_value,
    max(observation_value) as max_measure_value
from {{ ref('mart_cbs_observations') }}
where observation_value is not null
    and period_year is not null
group by
    measure_id,
    region_id,
    period_year
