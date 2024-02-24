with 

source as (

    select * from {{ source('staging', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,
        pickup_datetime,
        dropoff_datetime,
        pulocationid pickup_locationid,
        dolocationid dropoff_locationid,
        sr_flag,
        affiliated_base_number

    from source

)

select * from renamed
where  {{ dbt.date_trunc("year", "pickup_datetime") }} = '2019-01-01'

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
