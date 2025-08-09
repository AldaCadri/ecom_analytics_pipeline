{{ config(materialized='table') }}

select distinct
  survey_responseid      as user_key,
  age_group,
  is_hispanic,
  race,
  education,
  income_bracket,
  gender,
  sexual_orientation,
  user_state_name        as home_state_name,
  accounts_shared_cat,
  household_size_cat,
  purchase_frequency
from {{ ref('ref_orders_enriched') }}
