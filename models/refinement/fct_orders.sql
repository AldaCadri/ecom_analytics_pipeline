{{ config(materialized='table') }}

with enriched as (
  select
    survey_responseid      as user_key,
    order_date             as date_key,
    coalesce(final_fips, '00')      as state_key,     -- ‘00’ = unknown/digital
    coalesce(product_code, 'UNKNOWN') as product_key, -- catch any missing ASINs
    quantity,
    unit_price,
    order_value
  from {{ ref('ref_orders_enriched') }}
)

select * from enriched;

