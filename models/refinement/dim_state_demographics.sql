{{ config(materialized='table') }}

select distinct
  final_fips                     as state_fips,
  final_state_name              as state_name,
  order_year                    as survey_year,
  median_household_income,
  total_population
from {{ ref('ref_orders_enriched') }}
where final_fips is not null
  and median_household_income is not null
  and total_population is not null
