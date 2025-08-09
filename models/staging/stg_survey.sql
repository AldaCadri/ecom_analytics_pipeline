{{ config(materialized='view') }}

select
  survey_responseid,

  -- Age bucket as text
  trim(q_demos_age)   as age_group,

  -- Boolean value
  case
    when lower(q_demos_hispanic) in ('yes','true') then true
    when lower(q_demos_hispanic) in ('no','false') then false
    else null
  end                                                    as is_hispanic,

  -- Freeform text fields, trimmed & uppercased
  upper(trim(q_demos_race))                              as race,
  upper(trim(q_demos_education))                         as education,
  trim(q_demos_income)                                   as income_bracket,

  upper(trim(q_demos_gender))                            as gender,

  upper(trim(q_sexual_orientation))                      as sexual_orientation,
  upper(trim(q_demos_state))                             as state,

 
  nullif(trim(q_amazon_use_howmany), '')                 as accounts_shared_cat,
  nullif(trim(q_amazon_use_hh_size), '')                 as household_size_cat,
  nullif(trim(q_amazon_use_how_oft), '')                 as purchase_frequency,
  nullif(trim(q_substance_use_cigarettes), '')           as substance_use_cigarettes,
  nullif(trim(q_substance_use_marijuana), '')            as substance_use_marijuana,
  nullif(trim(q_substance_use_alcohol), '')              as substance_use_alcohol,
  nullif(trim(q_personal_diabetes), '')                  as personal_diabetes,
  nullif(trim(q_personal_wheelchair), '')                as personal_wheelchair,
  nullif(trim(q_life_changes), '')                       as life_changes,
  nullif(trim(q_sell_your_data), '')                     as sell_your_data,
  nullif(trim(q_sell_consumer_data), '')                 as sell_consumer_data,
  nullif(trim(q_small_biz_use), '')                      as small_biz_use,
  nullif(trim(q_census_use), '')                         as census_use,
  nullif(trim(q_research_society), '')                   as research_society

from {{ source('raw_data','raw_survey') }}
where survey_responseid is not null
