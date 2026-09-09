{{
  config(
    materialized = 'view',
    tags = ['staging']
  )
}}

/*
  stg_flights - thin 1:1 layer over Silver.

  Rules for this model, deliberately narrow:
    * rename and cast only
    * no joins, no filters, no aggregation, no business logic
    * one row in Silver == one row here

  Grain: one flight price quote (see fct_flight_price_quote for the full
  grain statement).
*/

with source as (

    select * from {{ source('silver', 'flights_valid') }}

),

renamed as (

    select
        -- keys
        flight_quote_sk,
        quote_group_sk,
        cast(as_of_date as date)                as as_of_date,

        -- descriptive attributes
        airline                                 as airline_name,
        flight                                  as flight_number,
        source_city                             as origin_city,
        destination_city                        as destination_city,
        route                                   as route_name,
        departure_time                          as departure_time_bucket,
        arrival_time                            as arrival_time_bucket,
        stops                                   as stops_label,
        cast(stops_count as integer)            as stops_count,
        cast(is_nonstop as boolean)             as is_nonstop,
        class                                   as cabin_class,

        -- measures
        cast(duration as double)                as duration_hours,
        cast(duration_minutes as integer)       as duration_minutes,
        cast(days_left as integer)              as days_to_departure,
        cast(price_inr as decimal(12,2))        as price_inr,

        -- lineage
        _source_file                            as source_file_name,
        cast(_source_row as integer)            as source_row_number,
        cast(_ingested_at as timestamp)         as ingested_at,
        _batch_id                               as batch_id

    from source

)

select * from renamed
