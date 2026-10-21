{{ config(severity='warn') }}

-- The first digit of the ZIP code must match the state: the test fails if this query returns rows
select
    u.user_id,
    u.state,
    u.zip_code
from {{ ref('stg_users') }} u
left join {{ ref('zip_regions') }} z
    on u.state = z.state
   and left(u.zip_code, 1) = z.zip_first_digit
where z.state is null
