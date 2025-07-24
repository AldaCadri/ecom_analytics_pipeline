{{ config(materialized='view') }}

with base as (
  select
    date_trunc('month', f.date_key)::date    as month,
    date_part('year', f.date_key)            as year,
    p.category                               as category,
    f.order_value
  from {{ ref('fct_orders') }}      f
  join {{ ref('dim_product')  }}   p
    on f.product_key = p.product_code

  -- Default to the last 3 full years for performance
  where date_part('year', f.date_key) >= date_part('year', current_date()) - 2  
)

select
  month,
  year,
  category,
  count(*)         as orders_count,
  sum(order_value) as total_revenue,
  avg(order_value) as avg_order_value
from base
group by 1,2,3
order by year desc, month desc, category
