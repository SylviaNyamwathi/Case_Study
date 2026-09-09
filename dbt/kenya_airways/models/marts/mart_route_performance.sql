{{
  config(
    materialized = 'table',
    tags = ['marts', 'gold']
  )
}}

/*
  GOLD MART 1 - Route Performance

  Business question: which routes are expensive, which are competitive, and
  where does fare dispersion suggest pricing opportunity?

  Business value: network and pricing teams use this to spot routes where a
  single carrier holds a price premium (few competitors, high avg fare) and
  routes where fares are compressed by competition. The min/max/spread columns
  matter more than the average - a wide spread on a busy route is where revenue
  management has room to move.

  Grain: one row per route x cabin class x snapshot date.
*/

with quotes as (

    select * from {{ ref('fct_flight_price_quote') }}

)

select
    q.as_of_date,
    q.route_id,
    q.route_name,
    q.origin_city,
    q.destination_city,
    q.cabin_class,

    -- volume / competition
    count(*)                                        as quote_count,
    count(distinct q.airline_name)                  as airline_count,
    count(distinct q.flight_number)                 as flight_number_count,

    -- fare distribution
    {{ inr('avg(q.price_inr)') }}                   as avg_price_inr,
    {{ inr('min(q.price_inr)') }}                   as min_price_inr,
    {{ inr('max(q.price_inr)') }}                   as max_price_inr,
    {{ inr('max(q.price_inr) - min(q.price_inr)') }} as price_spread_inr,
    {{ inr('stddev_samp(q.price_inr)') }}           as price_stddev_inr,
    {{ safe_divide('max(q.price_inr)', 'min(q.price_inr)') }} as max_to_min_price_ratio,

    -- schedule / product
    {{ safe_divide('avg(q.duration_hours)', '1') }} as avg_duration_hours,
    min(q.duration_hours)                           as fastest_duration_hours,
    {{ inr('avg(q.price_per_hour_inr)') }}          as avg_price_per_hour_inr,
    {{ pct('sum(case when q.is_nonstop then 1 else 0 end)', 'count(*)') }} as nonstop_share_pct

from quotes q
group by 1, 2, 3, 4, 5, 6
