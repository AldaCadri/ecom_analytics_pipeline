{{ config(materialized='table') }}

select distinct
  product_code,
  title,
  category,
  case
    when lower(title) like '%gift card%' then true
    else false
  end as is_gift_card
from {{ ref('ref_orders_enriched') }}
