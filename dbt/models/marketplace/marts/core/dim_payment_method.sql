{{ config(materialized='table') }}

-- Payment method dim with rollup of usage and chargeback rate.

with m as (
    select * from {{ ref('stg_payment_methods') }}
),
usage as (
    select
        method                                              as method_code,
        count(*)                                            as transactions,
        sum(amount)                                         as total_processed,
        sum(case when status = 'chargeback' then 1 else 0 end) * 1.0
            / nullif(count(*), 0)                           as chargeback_rate
    from {{ ref('stg_payments') }}
    group by method
)
select
    m.method_code,
    m.method_name,
    m.processor_fee_pct,
    m.settlement_days,
    m.chargeback_risk,
    coalesce(u.transactions, 0)        as transactions,
    coalesce(u.total_processed, 0)     as total_processed,
    coalesce(u.chargeback_rate, 0)     as chargeback_rate
from m
left join usage u using (method_code)
