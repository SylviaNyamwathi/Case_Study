{{
  config(
    materialized = 'ephemeral',
    tags = ['intermediate']
  )
}}

/*
  int_flight_routes - assigns a stable surrogate route_id.

  Directional on purpose: Delhi->Mumbai and Mumbai->Delhi are different
  commercial products with different demand curves and different fares, so they
  must not be collapsed. `route_pair_name` is provided alongside for the cases
  where an analyst genuinely wants the bidirectional city pair.

  Ephemeral: it is a lookup used by two downstream models and never queried
  directly, so there is no reason to persist it.
*/

with distinct_routes as (

    select distinct
        origin_city,
        destination_city,
        route_name
    from {{ ref('stg_flights') }}

)

select
    md5(origin_city || '||' || destination_city)    as route_id,
    origin_city,
    destination_city,
    route_name,
    -- alphabetised city pair, for bidirectional analysis
    case
        when origin_city < destination_city
            then origin_city || '<->' || destination_city
        else destination_city || '<->' || origin_city
    end                                             as route_pair_name
from distinct_routes
