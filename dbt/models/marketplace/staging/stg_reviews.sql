

with src as (
    select * from {{ source('marketplace_raw', 'reviews') }}
)
select
    review_id,
    order_id,
    customer_id,
    try_cast(rating as integer)                   as rating,
    title,
    body,
    try_cast(review_date as date)                 as review_date,
    try_cast(verified_purchase as boolean)        as verified_purchase
from src
