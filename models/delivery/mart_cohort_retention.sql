{{ config(materialized='view') }}

with first_order as (
  select
    user_key,
    min(date_key) as first_date
  from {{ ref('fct_orders') }}
  group by 1
),

-- derive cohort_month as the first day of the month of first_date
cohorts as (
  select
    fo.user_key,
    d_month.date_key as cohort_month
  from first_order fo
  join {{ ref('dim_date') }} d_month
    on fo.first_date = d_month.date_key
  where d_month.day_of_month = 1
),

events as (
  select
    f.user_key,
    c.cohort_month,
    datediff('month', c.cohort_month, f.date_key) as months_after
  from {{ ref('fct_orders') }} f
  join cohorts c
    on f.user_key = c.user_key
)

select
  cohort_month,
  months_after,
  count(distinct user_key) as active_users
from events
group by 1,2
order by 1,2