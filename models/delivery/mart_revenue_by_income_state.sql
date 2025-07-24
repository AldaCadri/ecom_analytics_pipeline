{{ config(materialized='view') }}

select
  u.income_bracket,
  s.state_fips         as state_key,
  count(*)             as orders_count,
  sum(f.order_value)   as total_revenue
from {{ ref('fct_orders') }}    f
join {{ ref('dim_user')     }}  u
  on f.user_key = u.user_key
join {{ ref('dim_state')    }}  s
  on f.state_key = s.state_fips
group by 1,2
order by total_revenue desc