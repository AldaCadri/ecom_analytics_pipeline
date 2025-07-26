{{ config(materialized='view') }}

with base as (
  select
    f.product_key,
    p.title,
    p.category,
    d.year                                  as year,
    sum(f.quantity)       as total_quantity,
    sum(f.order_value)    as total_revenue
  from {{ ref('fct_orders') }}       f
  left join {{ ref('dim_product') }} p on f.product_key = p.product_key
  left join {{ ref('dim_date') }}    d on f.date_key    = d.date_key
  group by 1,2,3,4
)

select * from base
limit 50