{{ config(materialized='table') }}

select
  postal_code as state_postal,
  state_name,
  state_fips
from {{ ref('stg_state_codes') }}
