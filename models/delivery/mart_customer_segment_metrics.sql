{{ config(materialized='view') }}

with dated_facts as (
  select
    f.user_key,
    f.quantity,
    f.order_value,
    d.year          as year
  from {{ ref('fct_orders') }} f
  join {{ ref('dim_date') }} d
    on f.date_key = d.date_key
),

with_user_segments as (
  select
    df.year,
    u.age_group,
    u.income_bracket,
    df.quantity,
    df.order_value
  from dated_facts df
  join {{ ref('dim_user') }} u
    on df.user_key = u.user_key
),

aggregated as (
  select
    age_group,
    income_bracket,
    year,
    count(*)            as orders_count,
    sum(quantity)       as total_units,
    sum(order_value)    as total_revenue,
    avg(order_value)    as avg_order_value
  from with_user_segments
  group by 1,2,3
)

select * from aggregated