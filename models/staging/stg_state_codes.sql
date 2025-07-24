-- models/staging/stg_state_codes.sql
{{ config(materialized='view') }}

select
  postal_code,
  state_name,
  state_fips
from {{ source('raw_data','raw_state_codes') }}
