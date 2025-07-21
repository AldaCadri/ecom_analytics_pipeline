{{ config(materialized='table') }}

select
  postal_code,
  state_name,
  state_fips
from {{ ref('stg_state_codes') }}
