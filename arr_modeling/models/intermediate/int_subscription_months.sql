-- Fan-out model: one row per subscription × active month.
--
-- A subscription is considered active in a given month when it is live on the
-- LAST DAY of that month:
--
--     start_date <= last_day(month)   -- started on or before month-end
--     end_date   >= last_day(month)   -- not yet expired by month-end (inclusive)
--
-- This handles subscriptions that start or end mid-month correctly:
-- a subscription starting on the 15th is active in that calendar month,
-- and a subscription expiring on the 8th is active in that calendar month.
--
-- Output: one row per (subscription_id, date_month) combination.

with subscriptions as (

    select * from {{ ref('stg_subscriptions') }}

),

date_spine as (

    select * from {{ ref('int_date_spine') }}

),

subscription_months as (

    select
        s.account_id,
        s.subscription_id,
        s.product_line,
        s.arr_usd,
        d.date_month

    from subscriptions  as s
    inner join date_spine as d
        on  s.start_date <= last_day(d.date_month)
        and s.end_date   >= last_day(d.date_month)

)

select * from subscription_months
