{{ config(materialized='table') }}

with bounds as (
  select
    min(order_date) as start_date,
    max(order_date) as end_date
  from {{ ref('ref_orders_enriched') }}
),

calendar as (
  select
    dateadd('day', seq4(), b.start_date) as date_key
  from bounds b
  cross join table(generator(rowcount => 3000))  -- covers ≈8 years (2018→2024)
),

filtered as (
  select
    c.date_key
  from calendar c
  cross join bounds b
  where c.date_key <= b.end_date
)

select
  date_key,
  date_part('year',    date_key)   as year,
  date_part('quarter', date_key)   as quarter,
  date_part('month',   date_key)   as month,
  date_part('day',     date_key)   as day_of_month,
  date_part('dow',     date_key)   as day_of_week,
  case when dayofweekiso(date_key) in (6,7) then true else false end as is_weekend,
  weekofyear(date_key)             as week_of_year
from filtered
order by date_key