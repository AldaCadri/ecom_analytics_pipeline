{{ config(materialized='view') }}

with orders as (
    select
        d.state_name,
        d.state_fips,
        dt.year,
        sum(f.order_value) as total_revenue,
        count(*) as total_orders
    from {{ ref('fct_orders') }} f
    join {{ ref('dim_state') }} d
        on f.state_key = d.state_fips
    join {{ ref('dim_date') }} dt
        on f.date_key = dt.date_key
    group by d.state_name, d.state_fips, dt.year
),

demographics as (
    select
        state_fips,
        state_name,
        survey_year as year,
        median_household_income,
        total_population
    from {{ ref('dim_state_demographics') }}
)

select
    o.state_name,
    o.state_fips,
    o.year,
    o.total_revenue,
    o.total_orders,
    d.median_household_income,
    d.total_population
from orders o
left join demographics d
  on o.state_fips = d.state_fips and o.year = d.year
