{{ config(materialized='view') }}

-- Pipeline health view — reads from the audit log written by the
-- persist_run_results macro. View (not table) so it always reflects the
-- LATEST runs, including the very run that built it.

with raw as (
    select * from main_audit.dbt_runs
),
runs_summary as (
    select
        run_id,
        invocation_at,
        max(git_sha)                                                  as git_sha,
        min(started_at)                                               as started_at,
        max(completed_at)                                             as completed_at,
        sum(duration_ms)                                              as total_duration_ms,
        count(*)                                                      as nodes_total,
        sum(case when status = 'success' then 1 else 0 end)           as nodes_success,
        sum(case when status = 'pass'    then 1 else 0 end)           as tests_pass,
        sum(case when status = 'warn'    then 1 else 0 end)           as tests_warn,
        sum(case when status in ('fail','error') then 1 else 0 end)   as nodes_error,
        sum(case when status = 'skipped' then 1 else 0 end)           as nodes_skipped
    from raw
    group by run_id, invocation_at
)
select
    run_id,
    substr(run_id, 1, 8)                              as run_short,
    git_sha,
    invocation_at,
    started_at,
    completed_at,
    total_duration_ms,
    total_duration_ms / 1000.0                        as total_duration_seconds,
    nodes_total,
    nodes_success + tests_pass                        as pass_count,
    tests_warn                                        as warn_count,
    nodes_error                                       as error_count,
    nodes_skipped                                     as skipped_count,
    case
        when nodes_error > 0 then 'failed'
        when tests_warn > 0  then 'warning'
        else                      'green'
    end                                               as overall_status
from runs_summary
order by invocation_at desc
