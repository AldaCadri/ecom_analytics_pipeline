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
    product_code,
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

-- Staged survey: gives user_state_name
users as (
  select
    survey_responseid,
    state               as user_state_name
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
    u.user_state_name,

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
