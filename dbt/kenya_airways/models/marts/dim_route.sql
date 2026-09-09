{{
  config(
    materialized = 'table',
    tags = ['marts', 'dimension']
  )
}}

/*
  dim_route - 30 rows (6 cities x 5 destinations; no same-city routes exist,
  and Silver rejects them if they ever appear).

  Carries route-level context an analyst needs before looking at any measure:
  how many carriers compete on it, whether nonstop service exists at all.
*/

with routes as (

    select * from {{ ref('int_flight_routes') }}

),

route_context as (

    select
        route_id,
        count(distinct airline_name)                            as competing_airline_count,
        max(case when is_nonstop then 1 else 0 end)             as has_nonstop_flag,
        min(duration_hours)                                     as fastest_duration_hours,
        count(*)                                                as quote_count
    from {{ ref('int_flight_pricing_enriched') }}
    group by 1

)

select
    r.route_id,
    r.origin_city,
    r.destination_city,
    r.route_name,
    r.route_pair_name,
    c.competing_airline_count,
    (c.has_nonstop_flag = 1)    as has_nonstop_service,
    c.fastest_duration_hours,
    c.quote_count
from routes r
left join route_context c
    on r.route_id = c.route_id
