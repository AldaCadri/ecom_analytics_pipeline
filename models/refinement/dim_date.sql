{{ config(materialized='table') }}

select distinct
  order_date             as date_key,
  order_year,
  order_quarter,
  order_month,
  order_dow              as day_of_week
from {{ ref('ref_orders_enriched') }}
