{{ config(materialized='view') }}

with base as (
  select
    date_trunc('month', f.date_key)::date    as month,
    date_part('year', f.date_key)            as year,
    s.state_fips                             as state_key,
    f.order_value
  from {{ ref('fct_orders') }}    f
  join {{ ref('dim_state')  }}    s
    on f.state_key = s.state_fips

  -- Default to the last 3 full years for performance
  where date_part('year', f.date_key) >= date_part('year', current_date()) - 2  
)

select
  month,
  year,
  state_key,
  count(*)         as orders_count,
  sum(order_value) as total_revenue,
  avg(order_value) as avg_order_value
from base
group by 1,2,3
order by year desc, month desc, state_key
