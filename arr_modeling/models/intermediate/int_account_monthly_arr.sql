-- Aggregates subscription-level ARR to account × month granularity.
--
-- Multiple subscriptions for the same account can be active in the same month
-- (different product lines or renewal periods). This model sums their ARR into
-- a single monthly_arr figure per account per month.
--
-- Also computes previous_month_arr via LAG so downstream models (fct_monthly_arr)
-- can derive arr_change and change_category without re-running window functions.
-- previous_month_arr is coalesced to 0 for the first month an account appears,
-- which correctly classifies that month as 'new' in the mart layer.
--
-- Output: one row per (account_id, date_month).

with subscription_months as (

    select * from {{ ref('int_subscription_months') }}

),

monthly_arr as (

    select
        account_id,
        date_month,
        sum(arr_usd) as monthly_arr

    from subscription_months
    group by account_id, date_month

),

with_previous_arr as (

    select
        account_id,
        date_month,
        monthly_arr,
        coalesce(
            lag(monthly_arr) over (
                partition by account_id
                order by date_month
            ),
            0
        ) as previous_month_arr

    from monthly_arr

)

select * from with_previous_arr
