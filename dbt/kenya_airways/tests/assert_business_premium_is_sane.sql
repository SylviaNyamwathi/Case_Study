-- Sanity guard on a derived ratio. A Business fare below Economy on the same
-- route, or a premium above 2000%, means the cabin split or the join broke.
select
    as_of_date,
    route_id,
    airline_name,
    avg_economy_price_inr,
    avg_business_price_inr,
    business_premium_pct
from {{ ref('mart_airline_cabin_mix') }}
where business_premium_pct is not null
  and (business_premium_pct <= 0 or business_premium_pct > 2000)
