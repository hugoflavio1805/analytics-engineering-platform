-- Singular test: a subscription cannot be 'active' if its end_date is in the past.
select subscription_id, status, end_date
from {{ ref('stg_subscriptions') }}
where status = 'active'
  and end_date is not null
  and end_date < current_date
