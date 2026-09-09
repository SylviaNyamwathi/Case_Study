{#
    Small reusable numeric helpers, so rounding and divide-by-zero handling are
    identical in every mart instead of being re-typed per model.
#}

{% macro safe_divide(numerator, denominator, precision=2) %}
    round(
        case
            when {{ denominator }} is null or {{ denominator }} = 0 then null
            else cast({{ numerator }} as double) / cast({{ denominator }} as double)
        end
    , {{ precision }})
{% endmacro %}


{% macro inr(column_name, precision=2) %}
    cast(round({{ column_name }}, {{ precision }}) as decimal(14,{{ precision }}))
{% endmacro %}


{% macro pct(numerator, denominator, precision=2) %}
    {{ safe_divide(numerator ~ ' * 100.0', denominator, precision) }}
{% endmacro %}
