{{
  config(
    materialized = 'table',
    tags = ['marts', 'data_quality']
  )
}}

/*
  GOLD MART 4 (supporting) - Data Quality Summary

  Not a business mart: this is the table a monitoring dashboard points at.
  One row per snapshot x rejection reason, plus the accepted-row count, so
  `rejected_pct` can be charted over time and alerted on (see
  documentation/monitoring.md - alert fires above 2%).
*/

with rejected as (

    select
        as_of_date,
        rejection_reason,
        count(*) as row_count
    from {{ ref('stg_flights_rejected') }}
    group by 1, 2

),

accepted as (

    select
        as_of_date,
        count(*) as accepted_row_count
    from {{ ref('stg_flights') }}
    group by 1

),

rejected_totals as (

    select as_of_date, sum(row_count) as rejected_row_count
    from rejected
    group by 1

)

select
    a.as_of_date,
    coalesce(r.rejection_reason, 'n/a')                     as rejection_reason,
    coalesce(r.row_count, 0)                                as rejected_row_count_for_reason,
    a.accepted_row_count,
    coalesce(t.rejected_row_count, 0)                       as rejected_row_count_total,
    a.accepted_row_count + coalesce(t.rejected_row_count, 0) as bronze_row_count_reconciled,
    {{ pct('coalesce(t.rejected_row_count, 0)', 'a.accepted_row_count + coalesce(t.rejected_row_count, 0)') }}
        as rejected_pct
from accepted a
left join rejected_totals t on a.as_of_date = t.as_of_date
left join rejected r        on a.as_of_date = r.as_of_date
