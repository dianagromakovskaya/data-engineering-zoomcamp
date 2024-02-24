{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
),
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.*
from fhv_tripdata
inner join dim_zones as pickup_zone
on fhv_tripdata.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_tripdata.dropoff_locationid = dropoff_zone.locationid


-- with green_tripdata as (
--     select *,
--         'Green' as service_type
--     from {{ ref('stg_green_tripdata') }}
-- ),
-- yellow_tripdata as(
--     select *,
--         'Yellow' as service_type
--     from {{ ref('stg_yellow_tripdata') }}
-- ),
-- trips_unioned as (
--     select * from green_tripdata
--     union all
--     select * from yellow_tripdata
-- )
-- select *
-- from trips_unioned