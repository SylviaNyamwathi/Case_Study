-- Aggregation guard: the mart must not lose or duplicate quotes.
-- sum(quote_count) in the mart == count(*) in the fact, per snapshot.
with mart as (
    select as_of_date, sum(quote_count) as n
    from {{ ref('mart_route_performance') }}
    group by 1
),
fact as (
    select as_of_date, count(*) as n
    from {{ ref('fct_flight_price_quote') }}
    group by 1
)
select
    f.as_of_date,
    f.n as fact_row_count,
    m.n as mart_quote_count
from fact f
full outer join mart m on f.as_of_date = m.as_of_date
where coalesce(f.n, -1) != coalesce(m.n, -1)
