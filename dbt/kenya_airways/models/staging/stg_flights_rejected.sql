{{
  config(
    materialized = 'view',
    tags = ['staging', 'data_quality']
  )
}}

/*
  stg_flights_rejected - the other half of the Silver split.

  Exposed as a model (not left as files on disk) so that data quality is
  queryable with the same tooling as the business marts, and so
  mart_data_quality_summary can be built and tested like any other mart.
*/

with source as (

    select * from {{ source('silver', 'flights_rejected') }}

)

select
    flight_quote_sk,
    cast(as_of_date as date)            as as_of_date,
    airline                             as airline_name,
    flight                              as flight_number,
    source_city                         as origin_city,
    destination_city                    as destination_city,
    class                               as cabin_class,
    cast(days_left as integer)          as days_to_departure,
    cast(price_inr as decimal(12,2))    as price_inr,
    cast(duration as double)            as duration_hours,
    rejection_reason,
    _source_file                        as source_file_name,
    cast(_ingested_at as timestamp)     as ingested_at,
    _batch_id                           as batch_id
from source
