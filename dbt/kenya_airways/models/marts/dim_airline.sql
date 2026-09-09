{{
  config(
    materialized = 'table',
    tags = ['marts', 'dimension']
  )
}}

/*
  dim_airline - 6 rows. Small enough for DISTSTYLE ALL in Redshift so that a
  join to the fact never shuffles data across slices.

  The carrier "profile" columns (is_low_cost, offers_business_class) are derived
  from the data rather than hard-coded: whether a carrier sells Business is a
  fact about the snapshot, not a fact I should assert from outside knowledge.
*/

with quotes as (

    select * from {{ ref('int_flight_pricing_enriched') }}

),

agg as (

    select
        airline_name,
        count(*)                                            as quote_count,
        count(distinct route_id)                            as route_count,
        count(distinct flight_number)                       as flight_number_count,
        max(case when is_premium_cabin then 1 else 0 end)   as offers_business_flag,
        {{ inr('avg(price_inr)') }}                         as avg_price_inr,
        {{ inr('min(price_inr)') }}                         as min_price_inr,
        {{ inr('max(price_inr)') }}                         as max_price_inr
    from quotes
    group by 1

)

select
    md5(airline_name)                       as airline_id,
    airline_name,
    replace(airline_name, '_', ' ')         as airline_display_name,
    (offers_business_flag = 1)              as offers_business_class,
    quote_count,
    route_count,
    flight_number_count,
    avg_price_inr,
    min_price_inr,
    max_price_inr
from agg
