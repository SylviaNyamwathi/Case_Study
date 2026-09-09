{{
  config(
    materialized = 'incremental',
    unique_key = 'flight_quote_sk',
    incremental_strategy = 'delete+insert',
    tags = ['marts', 'fact']
  )
}}

/*
  ==========================================================================
  GRAIN (stated identically in the README and documentation/data_model.md):

      One row = one price quote for one flight number, on one directional
      route, in one cabin class, at one booking lead time (days_to_departure),
      captured in one price snapshot (as_of_date).

  The natural key is therefore:
      flight_number + origin_city + destination_city + departure_time_bucket
      + arrival_time_bucket + cabin_class + days_to_departure + as_of_date
  surrogated as flight_quote_sk (md5) back in Silver.
  ==========================================================================

  Incremental strategy: delete+insert on flight_quote_sk, filtered to the
  snapshots that arrived since the last run. Because as_of_date is part of the
  key, a re-quote of the same flight on a later date APPENDS a new row (giving a
  price history) while a rerun of the SAME date replaces its own rows - so
  reruns cannot double-count. Late-arriving files are handled by the same
  mechanism: they carry their own as_of_date and slot into place.

  Backfill / full refresh: dbt run --full-refresh -s fct_flight_price_quote
*/

with enriched as (

    select * from {{ ref('int_flight_pricing_enriched') }}

    {% if is_incremental() %}
    -- Only reprocess snapshots at or after the newest one already loaded.
    -- ">=" not ">" on purpose: it lets a same-day rerun correct itself.
    where as_of_date >= (select coalesce(max(as_of_date), '1900-01-01') from {{ this }})
    {% endif %}

)

select
    -- keys
    e.flight_quote_sk,
    e.as_of_date,
    e.route_id,
    md5(e.airline_name)             as airline_id,

    -- degenerate / descriptive dimensions
    e.flight_number,
    e.airline_name,
    e.route_name,
    e.origin_city,
    e.destination_city,
    e.departure_time_bucket,
    e.arrival_time_bucket,
    e.stops_label,
    e.stops_count,
    e.is_nonstop,
    e.cabin_class,
    e.is_premium_cabin,
    e.lead_time_bucket,
    e.lead_time_bucket_sort,
    e.days_to_departure,

    -- measures
    e.price_inr,
    e.price_per_hour_inr,
    e.duration_hours,
    e.duration_minutes,

    -- lineage
    e.source_file_name,
    e.ingested_at,
    e.batch_id

from enriched e
