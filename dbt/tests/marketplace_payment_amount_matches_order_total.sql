-- Singular test: for non-failed payments, the captured amount must match
-- the order total (within $0.01). Catches the ~1% of rows where the source
-- system emitted a payment whose amount disagrees with the order ledger.
{{ config(severity='warn') }}

select
    o.order_id,
    o.total                           as order_total,
    p.amount                          as payment_amount,
    p.status                          as payment_status,
    abs(p.amount - o.total)           as delta
from {{ ref('fct_orders') }} o
join {{ ref('stg_payments') }} p using (order_id)
where p.status in ('captured','refunded')
  and abs(p.amount - o.total) > 0.01
