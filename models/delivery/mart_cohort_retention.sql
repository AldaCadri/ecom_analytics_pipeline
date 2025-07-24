{{ config(materialized='view') }}

with first_order as (
  select
    user_key,
    min(date_key) as cohort_date
  from {{ ref('fct_orders') }}
  group by 1
),

events as (
  select
    f.user_key,
    date_trunc('month', fo.cohort_date)                 as cohort_month,
    datediff('month', fo.cohort_date, f.date_key)      as months_after
  from {{ ref('fct_orders') }} f
  join first_order fo
    on f.user_key = fo.user_key
)

select
  cohort_month,
  months_after,
  count(distinct user_key) as active_users
from events
group by 1,2
order by 1,2
