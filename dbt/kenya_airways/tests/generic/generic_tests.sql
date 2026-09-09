{#
    Hand-rolled generic tests.

    dbt_utils and dbt_expectations would give these for free, but writing the
    two we actually need keeps the project installable and runnable with no
    external package fetch (`dbt build` works offline from a clean clone).
    packages.yml.example shows the package-based alternative.
#}

{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

    select *
    from {{ model }}
    where
        {{ column_name }} is not null
        and (
            false
            {% if min_value is not none %} or {{ column_name }} < {{ min_value }} {% endif %}
            {% if max_value is not none %} or {{ column_name }} > {{ max_value }} {% endif %}
        )

{% endtest %}


{% test unique_combination(model, combination_of_columns) %}

    {%- set cols = combination_of_columns | join(', ') -%}

    select {{ cols }}, count(*) as n
    from {{ model }}
    group by {{ cols }}
    having count(*) > 1

{% endtest %}
