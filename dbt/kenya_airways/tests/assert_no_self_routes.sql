-- Business rule: origin and destination cannot be the same city.
-- Enforced in Silver; asserted here so a future model change cannot reintroduce it.
select route_id, origin_city, destination_city
from {{ ref('dim_route') }}
where origin_city = destination_city
