with observations as (
    select *
    from {{ ref('mart_cbs_observations') }}
)

select
    observations.region_id,
    region.region_title,
    region.dimension_group_id as region_dimension_group_id,
    observations.period_year,
    observations.period_id,
    period.period_title,
    period.period_granularity,
    period.status as period_status,
    observations.measure_id,
    measure.measure_title,
    measure.measure_description,
    measure.data_type,
    measure.unit,
    measure.presentation_type,
    count(*) as observation_count,
    avg(observations.observation_value) as average_observation_value,
    min(observations.observation_value) as min_observation_value,
    max(observations.observation_value) as max_observation_value
from observations
left join {{ ref('dim_region') }} as region
    on observations.region_id = region.region_id
left join {{ ref('dim_period') }} as period
    on observations.period_id = period.period_id
left join {{ ref('dim_measure') }} as measure
    on observations.measure_id = measure.measure_id
where observations.observation_value is not null
    and observations.period_year is not null
group by
    observations.region_id,
    region.region_title,
    region.dimension_group_id,
    observations.period_year,
    observations.period_id,
    period.period_title,
    period.period_granularity,
    period.status,
    observations.measure_id,
    measure.measure_title,
    measure.measure_description,
    measure.data_type,
    measure.unit,
    measure.presentation_type
