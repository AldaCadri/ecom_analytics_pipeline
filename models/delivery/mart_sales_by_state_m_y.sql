{{ config(materialized='view') }}

select
  date_trunc('month', d.date_key)::date as month_label,         
  d.year,
  d.month,
  s.state_name         as state_name,    
  count(*)             as orders_count,
  sum(f.order_value)   as total_revenue,
  avg(f.order_value)   as avg_order_value
from {{ ref('fct_orders') }}   f
join {{ ref('dim_date')   }}   d
  on f.date_key = d.date_key
join {{ ref('dim_state')  }}   s
  on f.state_key = s.state_fips
group by 1,2,3,4
order by 2 desc, 1 desc, 4

