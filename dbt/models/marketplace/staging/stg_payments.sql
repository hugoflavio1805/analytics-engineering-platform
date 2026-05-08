{{ config(materialized='view') }}

with src as (
    select * from {{ source('marketplace_raw', 'payments') }}
)
select
    payment_id,
    order_id,
    lower(method)                                 as method,
    lower(status)                                 as status,
    try_cast(amount as decimal(12, 2))            as amount,
    upper(currency)                               as currency,
    try_cast(paid_at as timestamp)                as paid_at
from src
