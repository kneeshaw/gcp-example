{% macro rolling_avg(expr, date_col, partition_by, days=28) -%}
-- BigQuery: date-based rolling window; handles missing dates gracefully
AVG({{ expr }}) OVER (
  PARTITION BY {{ partition_by }}
  ORDER BY {{ date_col }}
  RANGE BETWEEN INTERVAL {{ days - 1 }} DAY PRECEDING AND CURRENT ROW
)
{%- endmacro %}
