{#
  Override dbt's default schema naming.

  Default dbt behavior writes custom-schema models to
  "<target_schema>_<custom_schema>" (e.g. CURATED_STAGED), which would
  not match the existing warehouse layout. This override uses the
  custom schema name verbatim, so models land in exactly
  MARKETPULSE.STAGED and MARKETPULSE.CURATED as the rest of the
  pipeline (Spark jobs, Streamlit app) expects.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
