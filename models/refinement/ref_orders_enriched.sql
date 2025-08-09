{{ config(materialized='table') }}

with

-- Base staged orders
orders as (
  select
    survey_responseid,
    order_date,
    state as shipping_postal,
    unit_price,
    quantity,
    coalesce(product_code, 'UNKNOWN') as product_key,
    title,
    category,
    order_year,
    order_month,
    order_quarter,
    order_dow,
    -- flag digital vs. physical
    case when state is null then true else false end as is_digital
  from {{ ref('stg_amazon_purchases') }}
),

-- 2) Staged survey: now selecting all demographic fields
users as (
  select
    survey_responseid,
    age_group,
    is_hispanic,
    race,
    education,
    income_bracket,
    gender,
    sexual_orientation,
    state  as user_state_name,
    accounts_shared_cat,
    household_size_cat,
    purchase_frequency
  from {{ ref('stg_survey') }}
),

-- Staged state codes: postal_code ↔ state_name ↔ state_fips
state_codes as (
  select * from {{ ref('stg_state_codes') }}
),

-- Staged state demographics: FIPS + year → income & population
state_demo as (
  select
    survey_year,
    state_fips,
    median_household_income,
    total_population
  from {{ ref('stg_state_demographics') }}
),

joined as (

  select
    o.*,
    u.age_group,
    u.is_hispanic,
    u.race,
    u.education,
    u.income_bracket,
    u.gender,
    u.sexual_orientation,
    u.user_state_name,
    u.accounts_shared_cat,
    u.household_size_cat,
    u.purchase_frequency,

    -- lookup shipping postal → state dimension
    s_ship.state_name   as shipping_state_name,
    s_ship.state_fips   as shipping_fips,

    -- lookup user residence full name → postal/fips
    s_home.postal_code  as home_postal,
    s_home.state_fips   as home_fips,
    s_home.state_name   as home_state_name,

    -- demographics by final FIPS
    d.median_household_income,
    d.total_population,

    -- business metric
    (o.unit_price * o.quantity)             as order_value,

    -- final unified keys
    coalesce(o.shipping_postal, s_home.postal_code)  as final_postal,
    coalesce(s_ship.state_name,   u.user_state_name) as final_state_name,
    coalesce(s_ship.state_fips,   s_home.state_fips) as final_fips

  from orders o

  left join users u
    on o.survey_responseid = u.survey_responseid

  left join state_codes s_ship
    on o.shipping_postal = s_ship.postal_code

  left join state_codes s_home
    on u.user_state_name  = s_home.state_name

  left join state_demo d
    on coalesce(s_ship.state_fips, s_home.state_fips) = d.state_fips
   and o.order_year = d.survey_year
)

select * from joined
