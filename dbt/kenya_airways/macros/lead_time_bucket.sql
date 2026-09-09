{#
    lead_time_bucket(column_name)

    Buckets booking lead time into the analytical bands used across the Gold
    layer. Boundaries come from `vars.lead_time_buckets` in dbt_project.yml, so
    changing the banding is a one-line change in one file rather than a
    find-and-replace across marts.

    Sort keys are provided by lead_time_bucket_sort() so BI tools order the
    bands chronologically instead of alphabetically.
#}

{% macro lead_time_bucket(column_name) %}
    {%- set b = var('lead_time_buckets') -%}
    case
        when {{ column_name }} <= {{ b['last_minute'] }} then '0-{{ b["last_minute"] }} days (last minute)'
        when {{ column_name }} <= {{ b['short'] }}       then '{{ b["last_minute"] + 1 }}-{{ b["short"] }} days (short)'
        when {{ column_name }} <= {{ b['medium'] }}      then '{{ b["short"] + 1 }}-{{ b["medium"] }} days (medium)'
        when {{ column_name }} <= {{ b['long'] }}        then '{{ b["medium"] + 1 }}-{{ b["long"] }} days (long)'
        else '{{ b["long"] + 1 }}+ days (advance)'
    end
{% endmacro %}


{% macro lead_time_bucket_sort(column_name) %}
    {%- set b = var('lead_time_buckets') -%}
    case
        when {{ column_name }} <= {{ b['last_minute'] }} then 1
        when {{ column_name }} <= {{ b['short'] }}       then 2
        when {{ column_name }} <= {{ b['medium'] }}      then 3
        when {{ column_name }} <= {{ b['long'] }}        then 4
        else 5
    end
{% endmacro %}
