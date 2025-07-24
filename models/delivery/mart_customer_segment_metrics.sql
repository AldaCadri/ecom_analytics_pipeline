{{ config(materialized='view') }}

select
  u.age_group,
  u.income_bracket,
  u.is_hispanic,
  u.household_size_cat,
  count(distinct f.user_key)                     as segment_users,
  sum(f.order_value)                             as segment_revenue,
  avg(f.order_value)                             as avg_order_value,
  count(*) / nullif(count(distinct f.user_key),0) as avg_orders_per_user
from {{ ref('fct_orders') }}   f
join {{ ref('dim_user')  }}    u
  on f.user_key = u.user_key
group by 1,2,3,4
order by segment_revenue desc