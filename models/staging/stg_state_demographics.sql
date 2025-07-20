{{ config(materialized='view') }}

with raw as (
  select
    survey_year,
    json_payload
  from {{ source('raw_data','raw_state_demographics') }}
),

parsed as (
  select
    survey_year,
    json_payload[0]::string  as state_name,
    json_payload[1]::integer as median_household_income,
    json_payload[2]::integer as total_population,
    -- PRESERVE the two-digit code exactly as in the JSON
    nullif(json_payload[3]::string, '') as state_fips
  from raw
),

clean as (
  select
    survey_year,
    state_name,
    median_household_income,
    total_population,
    state_fips
  from parsed
  where state_fips is not null
    and median_household_income >= 0
    and total_population > 0
)

select * from clean
