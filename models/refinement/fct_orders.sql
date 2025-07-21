{{ config(materialized='table') }}

select
  survey_responseid      as user_key,
  order_date             as date_key,
  final_fips             as state_key,
  product_code           as product_key,
  quantity,
  unit_price,
  order_value
from {{ ref('ref_orders_enriched') }}
