-- Cleaned orders with deterministic filtering rules:
--   - drop rows where order_date couldn't be parsed at all (will fail singular test)
--   - drop self-cancelled orders that never produced a payment (they are not "real" orders)
--   - keep everything else, even if there are downstream issues — flag, don't drop

with o as (
    select * from {{ ref('stg_orders') }}
),
p as (
    select order_id from {{ ref('stg_payments') }}
),
joined as (
    select
        o.*,
        case when p.order_id is null then false else true end   as has_payment
    from o
    left join p on o.order_id = p.order_id
)
select *
from joined
where order_date is not null
  and not (status = 'cancelled' and not has_payment)
