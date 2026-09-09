{{
  config(
    materialized = 'table',
    tags = ['marts', 'gold']
  )
}}

/*
  GOLD MART 2 - Booking Lead-Time Pricing

  Business question: how does fare move as departure approaches, and how does
  that curve differ by route and cabin?

  Business value: this is the mart that answers "when should we push fares up?"
  It compares each lead-time band against the cheapest band on the same route
  and cabin (`price_index_vs_cheapest_band`), so the shape of the curve is
  visible without the analyst having to compute a baseline. Commercial teams
  use it for advance-purchase fencing; the marketing team uses it to time
  campaigns into the bands where price sensitivity is highest.

  Grain: one row per route x cabin class x lead-time bucket x snapshot date.
*/

with quotes as (

    select * from {{ ref('fct_flight_price_quote') }}

),

by_band as (

    select
        as_of_date,
        route_id,
        route_name,
        cabin_class,
        lead_time_bucket,
        lead_time_bucket_sort,

        count(*)                                    as quote_count,
        min(days_to_departure)                      as min_days_to_departure,
        max(days_to_departure)                      as max_days_to_departure,
        {{ inr('avg(price_inr)') }}                 as avg_price_inr,
        {{ inr('min(price_inr)') }}                 as min_price_inr,
        {{ inr('max(price_inr)') }}                 as max_price_inr,
        {{ inr('avg(price_per_hour_inr)') }}        as avg_price_per_hour_inr

    from quotes
    group by 1, 2, 3, 4, 5, 6

),

with_baseline as (

    select
        b.*,
        min(b.avg_price_inr) over (
            partition by b.as_of_date, b.route_id, b.cabin_class
        ) as cheapest_band_avg_price_inr,
        first_value(b.avg_price_inr) over (
            partition by b.as_of_date, b.route_id, b.cabin_class
            order by b.lead_time_bucket_sort desc
        ) as advance_band_avg_price_inr
    from by_band b

)

select
    as_of_date,
    route_id,
    route_name,
    cabin_class,
    lead_time_bucket,
    lead_time_bucket_sort,
    quote_count,
    min_days_to_departure,
    max_days_to_departure,
    avg_price_inr,
    min_price_inr,
    max_price_inr,
    avg_price_per_hour_inr,
    cheapest_band_avg_price_inr,

    -- 100 = as cheap as the cheapest band on this route/cabin; 180 = 80% dearer
    {{ safe_divide('avg_price_inr * 100.0', 'cheapest_band_avg_price_inr') }}
        as price_index_vs_cheapest_band,

    -- uplift vs booking far in advance, the number commercial actually quotes
    {{ safe_divide('(avg_price_inr - advance_band_avg_price_inr) * 100.0', 'advance_band_avg_price_inr') }}
        as pct_uplift_vs_advance_band

from with_baseline
