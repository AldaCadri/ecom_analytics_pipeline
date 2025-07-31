{{ config(materialized='view') }}

with first_orders as (
  select
    user_key,
    date_trunc('month', min(date_key)) as cohort_month
  from {{ ref('fct_orders') }}
  group by user_key
),

all_orders as (
  select
    fo.cohort_month,
    o.user_key,
    date_trunc('month', o.date_key) as order_month,
    datediff(
      month,
      fo.cohort_month,
      date_trunc('month', o.date_key)
    ) as months_after
  from first_orders fo
  join {{ ref('fct_orders') }} o
    on fo.user_key = o.user_key
),

retention as (
  select
    cohort_month,
    months_after,
    count(distinct user_key) as active_users
  from all_orders
  group by 1, 2
),

cohort_sizes as (
  select
    cohort_month,
    count(distinct user_key) as cohort_size
  from first_orders
  group by 1
)

select
  r.cohort_month,
  d.year as cohort_year,
  r.months_after,
  r.active_users,
  cs.cohort_size,
  round(100.0 * r.active_users / cs.cohort_size, 1) as retention_pct
from retention r
join cohort_sizes cs
  on r.cohort_month = cs.cohort_month
join {{ ref('dim_date') }} d
  on r.cohort_month = d.date_key
order by cohort_month, months_after


