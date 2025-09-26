{% macro run_sql(sql) %}
    {% set results = run_query(sql) %}

    {% if execute %}
        {% if results is none %}
            {{ log("No results returned.", info=True) }}
        {% else %}
            {% set table = results.columns | map(attribute='name') | list %}
            {{ log(" | ".join(table), info=True) }}
            {% for row in results.rows %}
                {{ log(" | ".join(row | map("string") | list), info=True) }}
            {% endfor %}
        {% endif %}
    {% endif %}
{% endmacro %}


{# 
  Macro: run_sql
  Purpose: Run arbitrary SQL queries from the CLI 
  Usage: dbt run-operation run_sql --args '{"sql": "select count(*) from public.stg_tracks;"}' 
#}