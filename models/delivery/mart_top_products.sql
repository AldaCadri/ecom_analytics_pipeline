{{ config(materialized='view') }}

select
  f.product_key,
  p.category,
  sum(f.quantity)      as units_sold,
  sum(f.order_value)   as total_revenue
from {{ ref('fct_orders') }}   f
join {{ ref('dim_product') }}  p
  on f.product_key = p.product_key
group by 1,2
order by total_revenue desc
limit 50