{{ config(materialized='view') }}

select
    lower(method_code)                              as method_code,
    method_name,
    try_cast(processor_fee_pct as decimal(5,4))     as processor_fee_pct,
    try_cast(settlement_days as integer)            as settlement_days,
    chargeback_risk
from {{ source('marketplace_raw', 'payment_methods') }}
