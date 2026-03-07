-- Aggregates subscription-level ARR to account × month granularity and
-- prepares the full dataset (real rows + synthetic churn row) for mart classification.
--
-- Pipeline:
--   1. Aggregate: sum ARR across all active subscriptions per account × month.
--   2. Bounds: compute first_active_month / last_active_month per account via
--      window functions. These are needed to distinguish 'new' from 'reactivation'
--      in the mart and to identify where to append the synthetic churn row.
--   3. Synthetic churn: append one row per account for the calendar month
--      immediately after the last active month (monthly_arr = 0). This makes
--      the churn event explicit in the data — without it, the last real month
--      would simply be the final row with no signal that the account stopped.
--      NOTE: this does NOT fill gaps between subscriptions; it only marks
--      the single month when the account transitioned from active to inactive.
--   4. LAG: compute previous_month_arr over the full combined dataset (real +
--      synthetic). Running LAG after the UNION ALL means the synthetic churn row
--      automatically receives the correct previous value from the last real month,
--      with no manual override needed.
--
-- Output: one row per (account_id, date_month), including one synthetic churn row
-- per account. Consumed by fct_monthly_arr, which only adds classification logic.

with subscription_months as (

    select * from {{ ref('int_subscription_months') }}

),

-- Step 1 — Aggregate subscription-level ARR to account × month.
-- Multiple subscriptions can be active simultaneously (different product lines
-- or overlapping renewals); sum them into a single monthly figure.
monthly_arr as (

    select
        account_id,
        date_month,
        sum(arr_usd) as monthly_arr

    from subscription_months
    group by account_id, date_month

),

-- Step 2 — Identify the first and last active month per account.
-- first_active_month → used by fct_monthly_arr to classify the 'new' category.
-- last_active_month  → used below to place the synthetic churn row.
with_bounds as (

    select
        account_id,
        date_month,
        monthly_arr,
        min(date_month) over (partition by account_id) as first_active_month,
        max(date_month) over (partition by account_id) as last_active_month

    from monthly_arr

),

-- Step 3 — Synthetic churn row: one row per account for the month after the
-- last active month, with monthly_arr = 0.
-- Without this row, churn would never appear in fct_monthly_arr because
-- the subscription data simply has no row for that period.
synthetic_churn as (

    select
        account_id,
        (date_month + interval '1 month')::date as date_month,
        0.0                                      as monthly_arr,
        first_active_month,
        last_active_month

    from with_bounds
    where date_month = last_active_month

),

-- Step 4 — Combine real rows and the synthetic churn row.
combined as (

    select account_id, date_month, monthly_arr, first_active_month, last_active_month
    from with_bounds

    union all

    -- Synthetic churn row (arr = 0, month after last active).
    select account_id, date_month, monthly_arr, first_active_month, last_active_month
    from synthetic_churn

),

-- Step 5 — Compute previous_month_arr via LAG over the full combined dataset.
-- Running the window here (after the UNION ALL) ensures the synthetic churn row
-- receives the correct value from the last real month automatically.
-- coalesce to 0 for the first month an account appears (no prior row).
with_lag as (

    select
        account_id,
        date_month,
        monthly_arr,
        first_active_month,
        coalesce(
            lag(monthly_arr) over (
                partition by account_id
                order by date_month
            ),
            0
        ) as previous_month_arr

    from combined

)

select * from with_lag
