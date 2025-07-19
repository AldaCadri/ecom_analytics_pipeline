{{ config(materialized='view') }}

select
  survey_responseid,

  -- Age to integer
  try_cast(q_demos_age as integer) as age,

  -- Boolean flags
  lower(q_demos_hispanic)   in ('yes','true') as is_hispanic,
  lower(q_substance_use_cigarettes) in ('yes','true') as uses_cigarettes,
  lower(q_substance_use_marijuana)  in ('yes','true') as uses_marijuana,
  lower(q_substance_use_alcohol)    in ('yes','true') as uses_alcohol,
  lower(q_personal_diabetes)        in ('yes','true') as has_diabetes,
  lower(q_personal_wheelchair)      in ('yes','true') as uses_wheelchair,

  -- String normalization
  upper(trim(q_demos_race))           as race,
  upper(trim(q_demos_education))      as education,
  trim(q_demos_income)                as income_bracket,
  case
    when lower(q_demos_gender) in ('m','male')   then 'MALE'
    when lower(q_demos_gender) in ('f','female') then 'FEMALE'
    else 'OTHER'
  end                                   as gender,
  upper(trim(q_sexual_orientation))     as sexual_orientation,
  upper(trim(q_demos_state))            as state,

  -- Household counts
  try_cast(q_amazon_use_howmany as integer) as accounts_shared,
  try_cast(q_amazon_use_hh_size as integer) as household_size,

  -- Frequency bin as-is
  trim(q_amazon_use_how_oft)            as purchase_frequency,

  -- Open-text (nullable)
  nullif(trim(q_life_changes), '')      as life_changes,

  -- Consent/use flags
  lower(q_sell_your_data)    in ('yes','true') as sell_own_data,
  lower(q_sell_consumer_data)in ('yes','true') as sell_consumer_data,
  lower(q_small_biz_use)     in ('yes','true') as small_biz_use,
  lower(q_census_use)        in ('yes','true') as census_use,
  lower(q_research_society)  in ('yes','true') as research_society

from {{ source('raw_data','raw_survey') }}
where survey_responseid is not null
