{{
  config(
    materialized = 'table',
    tags = ['marts', 'gold']
  )
}}

/*
  GOLD MART 3 - Airline & Cabin Mix

  Business question: who competes where, and what does the Business-over-Economy
  premium look like by carrier and route?

  Business value: two things at once. `route_share_pct` shows each carrier's
  share of quoted capacity on a route - the competitive picture. And
  `business_premium_pct` quantifies how much more a carrier charges for Business
  on the same route, which is the input to cabin-mix and upsell decisions. A
  carrier with high share but a thin premium is leaving money on the table;
  a thin-share carrier with a fat premium is running a niche premium product.

  Grain: one row per airline x route x snapshot date (cabin split into columns,
  because the premium is a ratio between cabins and belongs on one row).
*/

with quotes as (

    select * from {{ ref('fct_flight_price_quote') }}

),

by_airline_route as (

    select
        as_of_date,
        route_id,
        route_name,
        airline_name,

        count(*)                                                    as quote_count,
        count(distinct flight_number)                               as flight_number_count,
        sum(case when cabin_class = 'Economy'  then 1 else 0 end)   as economy_quote_count,
        sum(case when cabin_class = 'Business' then 1 else 0 end)   as business_quote_count,

        {{ inr("avg(case when cabin_class = 'Economy'  then price_inr end)") }}
            as avg_economy_price_inr,
        {{ inr("avg(case when cabin_class = 'Business' then price_inr end)") }}
            as avg_business_price_inr,
        {{ inr('avg(price_inr)') }}                                 as avg_price_inr,
        {{ pct('sum(case when is_nonstop then 1 else 0 end)', 'count(*)') }}
            as nonstop_share_pct

    from quotes
    group by 1, 2, 3, 4

)

select
    a.as_of_date,
    a.route_id,
    a.route_name,
    a.airline_name,
    a.quote_count,
    a.flight_number_count,
    a.economy_quote_count,
    a.business_quote_count,
    a.avg_economy_price_inr,
    a.avg_business_price_inr,
    a.avg_price_inr,
    a.nonstop_share_pct,

    -- share of all quoted flights on this route, this snapshot
    {{ pct('a.quote_count', 'sum(a.quote_count) over (partition by a.as_of_date, a.route_id)') }}
        as route_share_pct,

    -- Business vs Economy premium. NULL where the carrier sells only one cabin,
    -- which is correct: a premium of 0 would imply parity that does not exist.
    {{ safe_divide(
        '(a.avg_business_price_inr - a.avg_economy_price_inr) * 100.0',
        'a.avg_economy_price_inr'
    ) }} as business_premium_pct,

    (a.business_quote_count > 0)    as sells_business_cabin

from by_airline_route a
