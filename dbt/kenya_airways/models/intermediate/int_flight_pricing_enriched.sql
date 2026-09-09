{{
  config(
    materialized = 'ephemeral',
    tags = ['intermediate']
  )
}}

/*
  int_flight_pricing_enriched - the single place where quote-level derived
  measures are defined.

  Everything the marts need beyond raw Silver columns is computed once here:
    * route_id                (joined from int_flight_routes)
    * lead_time_bucket        (macro-driven, boundaries from dbt_project vars)
    * price_per_hour          (fare normalised by flight duration - lets a
                               2h nonstop and a 12h two-stop be compared)
    * is_premium_cabin        (Business flag, used for cabin-mix and premium %)

  Defining these once is why the three marts can never disagree about what a
  "medium lead time" or a "price per hour" is.
*/

with flights as (

    select * from {{ ref('stg_flights') }}

),

routes as (

    select * from {{ ref('int_flight_routes') }}

)

select
    f.flight_quote_sk,
    f.as_of_date,
    r.route_id,
    f.route_name,
    r.route_pair_name,
    f.origin_city,
    f.destination_city,
    f.airline_name,
    f.flight_number,
    f.departure_time_bucket,
    f.arrival_time_bucket,
    f.stops_label,
    f.stops_count,
    f.is_nonstop,
    f.cabin_class,
    f.duration_hours,
    f.duration_minutes,
    f.days_to_departure,
    f.price_inr,

    -- derived measures
    {{ lead_time_bucket('f.days_to_departure') }}       as lead_time_bucket,
    {{ lead_time_bucket_sort('f.days_to_departure') }}  as lead_time_bucket_sort,
    {{ safe_divide('f.price_inr', 'f.duration_hours') }} as price_per_hour_inr,
    (f.cabin_class = 'Business')                        as is_premium_cabin,

    f.source_file_name,
    f.ingested_at,
    f.batch_id

from flights f
inner join routes r
    on f.origin_city = r.origin_city
   and f.destination_city = r.destination_city
