

select
    carrier_id,
    carrier_name,
    upper(origin_country)                          as origin_country,
    try_cast(sla_promised_days as integer)         as sla_promised_days,
    try_cast(reliability_score as decimal(3,2))    as reliability_score
from {{ source('marketplace_raw', 'carriers') }}
