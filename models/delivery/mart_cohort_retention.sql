{{ config(materialized='view') }}

with first_orders as (
    select
        user_key,
        min(date_key) as cohort_date_key
    from {{ ref('fct_orders') }}
    group by user_key
),

all_orders as (
    select
        fo.cohort_date_key,
        o.user_key,
        o.date_key,
        datediff(
            month,
            date_trunc('month', fo.cohort_date_key),
            date_trunc('month', o.date_key)
        ) as months_after
    from first_orders fo
    join {{ ref('fct_orders') }} o
        on fo.user_key = o.user_key
),

retention as (
    select
        cohort_date_key,
        months_after,
        count(distinct user_key) as active_users
    from all_orders
    group by 1, 2
),

cohort_sizes as (
    select
        cohort_date_key,
        count(distinct user_key) as cohort_size
    from all_orders
    where months_after = 0
    group by 1
),

final as (
    select
        to_date(r.cohort_date_key) as cohort_month,
        dd.year as cohort_year,
        r.months_after,
        r.active_users,
        cs.cohort_size,
        round(100.0 * r.active_users / cs.cohort_size, 1) as retention_pct
    from retention r
    join cohort_sizes cs on r.cohort_date_key = cs.cohort_date_key
    join {{ ref('dim_date') }} dd on r.cohort_date_key = dd.date_key
)

select *
from final
order by cohort_month, months_after
