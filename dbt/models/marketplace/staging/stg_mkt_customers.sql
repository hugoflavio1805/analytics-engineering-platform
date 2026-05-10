

with src as (
    select * from {{ source('marketplace_raw', 'customers') }}
)
select
    customer_id,
    full_name,
    nullif(trim(email), '')                      as email,
    -- email validity: very permissive — needs an @ and a dot
    case
        when nullif(trim(email), '') is null then false
        when email like '%@%.%' then true
        else false
    end                                          as has_valid_email,
    upper(trim(country))                         as country,
    try_cast(signup_date as date)                as signup_date,
    try_cast(is_prime as boolean)                as is_prime
from src
