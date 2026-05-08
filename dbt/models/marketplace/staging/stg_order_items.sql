{{ config(materialized='view') }}

with src as (
    select * from {{ source('marketplace_raw', 'order_items') }}
)
select
    order_id,
    try_cast(line_no as integer)                  as line_no,
    product_id,
    upper(replace(trim(seller_id), '_', '-'))     as seller_id,
    try_cast(quantity as integer)                 as quantity,
    try_cast(unit_price as decimal(12, 2))        as unit_price,
    try_cast(line_total as decimal(12, 2))        as line_total
from src
