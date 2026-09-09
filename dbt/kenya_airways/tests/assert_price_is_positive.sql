-- Silver already rejects non-positive fares. This retests the invariant at the
-- Gold boundary, because "it was true upstream" is an assumption, not a control.
select
    flight_quote_sk,
    price_inr
from {{ ref('fct_flight_price_quote') }}
where price_inr is null or price_inr <= 0
