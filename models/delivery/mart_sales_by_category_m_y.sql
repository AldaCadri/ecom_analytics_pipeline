{{ config(materialized='view') }}

select
  d.date_key           as month,        
  d.year,                            
  p.category            as category,    
  count(*)               as orders_count,
  sum(f.order_value)     as total_revenue,
  avg(f.order_value)     as avg_order_value
from {{ ref('fct_orders') }}     f
join {{ ref('dim_date')    }}     d
  on f.date_key = d.date_key
join {{ ref('dim_product') }}     p
  on f.product_key = p.product_key

where d.year >= date_part('year', current_date()) - 2  

group by 1,2,3
order by 2 desc, 1 desc, 3
