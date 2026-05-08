{# Wrapper so models stay portable between Snowflake (native QUALIFY) and engines without it.
   DuckDB and Snowflake both support QUALIFY — this macro is a placeholder for the
   Redshift port (where we'd wrap the model in a CTE + WHERE rn = 1). #}

{% macro qualify_dedupe(partition_by, order_by) %}
    {{ return("qualify row_number() over (partition by " ~ partition_by ~ " order by " ~ order_by ~ ") = 1") }}
{% endmacro %}
