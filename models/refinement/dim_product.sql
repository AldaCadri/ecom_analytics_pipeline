{{ config(materialized='table') }}

with raw_products as (
  select
    coalesce(product_key,'UNKNOWN') as product_key,
    title,
    category,
    case when lower(title) like '%gift card%' then true else false end as is_gift_card
  from {{ ref('ref_orders_enriched') }}
),

-- count how often each (key, title, category, flag) appears
counts as (
  select
    product_key,
    title,
    category,
    is_gift_card,
    count(*) as cnt
  from raw_products
  group by 1,2,3,4
),

-- rank them per product_key by frequency
ranked as (
  select
    *,
    row_number() over(
      partition by product_key 
      order by cnt desc
    ) as rn
  from counts
)

-- pick only the top‐ranked row per product_key
select
  product_key,
  title,
  category,
  is_gift_card
from ranked
where rn = 1
