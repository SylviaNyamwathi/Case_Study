-- Source-to-target reconciliation across the dbt boundary:
-- every valid Silver row must appear exactly once in the fact, and the fact
-- must invent nothing. Returns rows only when the counts disagree.
with staging as (
    select count(*) as n from {{ ref('stg_flights') }}
),
fact as (
    select count(*) as n from {{ ref('fct_flight_price_quote') }}
)
select
    staging.n as staging_row_count,
    fact.n    as fact_row_count
from staging
cross join fact
where staging.n != fact.n
