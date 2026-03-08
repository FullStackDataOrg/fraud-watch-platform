{% macro rolling_window(column, agg_fn, days, partition_by) %}
    {{ agg_fn }}({{ column }}) over (
        partition by {{ partition_by }}
        order by event_at
        range between interval '{{ days }}' day preceding and current row
    )
{% endmacro %}
