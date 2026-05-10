{# Captures every dbt run/test result and persists it.

   The macro runs after every node finishes (configured via on-run-end in
   dbt_project.yml). It writes a row per node with:

     run_id (sha of invocation_id)
     started_at, finished_at, duration_ms
     node_id, resource_type (model/test/seed/source)
     status (success/error/skipped/warn)
     rows_affected
     git_sha (from env var GIT_SHA if set)

   Persists into the audit schema. The first run creates the table; subsequent
   runs append.
#}

{# Pre-build hook: ensure the audit schema and table exist BEFORE any model
   that reads from it (notably pipeline_health). #}
{% macro ensure_audit_table() %}
    {% if execute %}
        {% set ddl %}
            create schema if not exists main_audit;
            create table if not exists main_audit.dbt_runs (
                run_id          varchar,
                git_sha         varchar,
                invocation_at   timestamp,
                started_at      timestamp,
                completed_at    timestamp,
                duration_ms     bigint,
                node_id         varchar,
                node_name       varchar,
                resource_type   varchar,
                package_name    varchar,
                status          varchar,
                message         varchar,
                rows_affected   bigint
            );
        {% endset %}
        {% do run_query(ddl) %}
    {% endif %}
{% endmacro %}


{% macro persist_run_results() %}
    {% if execute %}
        {% set rows = [] %}
        {% set git_sha = env_var('GIT_SHA', 'unknown') %}
        {% set run_id = invocation_id %}

        {% for result in results %}
            {% set node = result.node %}
            {% set started = result.timing | selectattr('name', 'eq', 'execute') | map(attribute='started_at') | first %}
            {% set completed = result.timing | selectattr('name', 'eq', 'execute') | map(attribute='completed_at') | first %}
            {% do rows.append({
                'run_id':         run_id,
                'git_sha':        git_sha,
                'invocation_at':  run_started_at | string,
                'started_at':     started | string if started else '',
                'completed_at':   completed | string if completed else '',
                'duration_ms':    (result.execution_time * 1000) | round(0, 'floor'),
                'node_id':        node.unique_id,
                'node_name':      node.name,
                'resource_type':  node.resource_type,
                'package_name':   node.package_name,
                'status':         result.status | string,
                'message':        (result.message | string) if result.message else '',
                'rows_affected':  result.adapter_response.get('rows_affected', 0) if result.adapter_response else 0
            }) %}
        {% endfor %}

        {% if rows | length == 0 %}
            {% do log("persist_run_results: no results to record", info=true) %}
            {{ return('') }}
        {% endif %}

        -- Build a VALUES clause for an INSERT
        {% set value_rows = [] %}
        {% for r in rows %}
            {% do value_rows.append(
                "('" ~ r.run_id ~ "', "
                ~ "'" ~ r.git_sha ~ "', "
                ~ "TIMESTAMP '" ~ r.invocation_at ~ "', "
                ~ ("TIMESTAMP '" ~ r.started_at ~ "'" if r.started_at else "NULL") ~ ", "
                ~ ("TIMESTAMP '" ~ r.completed_at ~ "'" if r.completed_at else "NULL") ~ ", "
                ~ r.duration_ms | int ~ ", "
                ~ "'" ~ r.node_id ~ "', "
                ~ "'" ~ r.node_name ~ "', "
                ~ "'" ~ r.resource_type ~ "', "
                ~ "'" ~ r.package_name ~ "', "
                ~ "'" ~ r.status ~ "', "
                ~ "'" ~ (r.message | replace("'", "''")) ~ "', "
                ~ r.rows_affected | int
                ~ ")"
            ) %}
        {% endfor %}

        {% set create_sql %}
            create schema if not exists main_audit;
            create table if not exists main_audit.dbt_runs (
                run_id          varchar,
                git_sha         varchar,
                invocation_at   timestamp,
                started_at      timestamp,
                completed_at    timestamp,
                duration_ms     bigint,
                node_id         varchar,
                node_name       varchar,
                resource_type   varchar,
                package_name    varchar,
                status          varchar,
                message         varchar,
                rows_affected   bigint
            );
            insert into main_audit.dbt_runs values
            {{ value_rows | join(',\n') }};
        {% endset %}

        {% do run_query(create_sql) %}
        {% do log("persist_run_results: inserted " ~ rows | length ~ " rows into main_audit.dbt_runs", info=true) %}
    {% endif %}
{% endmacro %}
