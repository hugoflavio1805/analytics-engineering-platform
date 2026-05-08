-- Singular test:
-- After id normalization, every feedback's customer_id must exist in dim_customer.
-- This catches the regression where feedbacks reference 'CUST_0178' but customer master has 'CUST-0178'.

select f.feedback_id, f.customer_id
from {{ ref('stg_feedbacks') }} f
left join {{ ref('dim_customer') }} c on f.customer_id = c.customer_id
where c.customer_id is null
  and f.customer_id is not null
