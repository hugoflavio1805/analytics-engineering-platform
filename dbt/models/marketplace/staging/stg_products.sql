

with src as (
    select * from {{ source('marketplace_raw', 'products') }}
)
select
    product_id,
    sku,
    product_name,
    category,
    nullif(trim(category_id), '')                as category_id,
    upper(replace(trim(seller_id), '_', '-'))    as seller_id,
    try_cast(unit_price as decimal(12, 2))       as unit_price,
    try_cast(stock_qty as integer)               as stock_qty,
    try_cast(created_at as date)                 as created_at,

    try_cast(unit_price as decimal(12, 2)) < 0   as has_negative_price,
    try_cast(stock_qty as integer) < 0           as has_negative_stock
from src
