{{ config(materialized='view') }}

with raw as (
  select
    survey_responseid,
    order_date::date                                        as order_date,
    nullif(trim(shipping_address_state), '')                as state,
    CAST(purchase_price_per_unit AS numeric(10,2))          as unit_price,
    CAST(quantity                AS integer)                as quantity,
    trim(asin_isbn_prod_code)                               as product_code,
    nullif(trim(title), '')                                 as title,
    coalesce(
    nullif(upper(category), ''),    -- uppercase non‐empty
    'UNKNOWN'                       -- replace empty or null
  ) as category
  from {{ source('raw_data','raw_amazon_purchases') }}
),

deduped as (
  select
    *,
    row_number() over (
      partition by
        survey_responseid,
        order_date,
        state,
        unit_price,
        quantity,
        product_code
      order by survey_responseid
    ) as rn
  from raw
),

clean as (
  select
    survey_responseid,
    order_date,
    state,
    unit_price,
    quantity,
    product_code,
    title,
    category,
    -- derived date parts for downstream use
    extract(year  from order_date) as order_year,
    extract(month from order_date) as order_month,
    extract(quarter from order_date) as order_quarter,
    to_char(order_date,'DY')        as order_dow
  from deduped
  where rn = 1
    and survey_responseid is not null
    and order_date between '2018-01-01' and current_date()
    and unit_price  > 0
    and quantity    > 0
)

select * from clean
