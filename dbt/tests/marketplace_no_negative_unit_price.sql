-- Singular test: a product's unit_price must be > 0.
-- Catches the ~1% of rows the source emits with negative prices
-- (data quality issue exposed by the dim_product.has_negative_price flag).
{{ config(severity='warn') }}

select
    product_id,
    sku,
    unit_price
from {{ ref('dim_product') }}
where unit_price <= 0
