-- Aggregates subscription-level ARR to account × month granularity and
-- fills account-month gaps so interim churn/reactivation are modeled explicitly.
--
-- Pipeline:
--   1. Aggregate real ARR by account × month from active subscriptions.
--   2. Compute per-account bounds (first and last active month).
--   3. Build a continuous month series per account between those bounds.
--   4. Left join real ARR onto the continuous series and coalesce gaps to 0.
--   5. Compute previous_month_arr with LAG over the gap-filled series.
--
-- Why:
--   - If an account is active in Oct, inactive in Nov, and active again in Dec,
--     Nov must exist as monthly_arr = 0 so the mart can classify:
--       Nov -> churn (0 from >0)
--       Dec -> reactivation (>0 from 0)

with subscription_months as (

    select * from {{ ref('int_subscription_months') }}

),

-- Step 1 — Aggregate subscription-level ARR to account × month.
monthly_arr as (

    select
        account_id,
        date_month,
        sum(arr_usd) as monthly_arr

    from subscription_months
    group by account_id, date_month

),

-- Step 2 — Per-account active bounds used to generate continuous month series.
account_bounds as (

    select
        account_id,
        min(date_month) as first_active_month,
        max(date_month) as last_active_month

    from monthly_arr
    group by account_id

),

-- Step 3 — Continuous account × month series between first and last active month.
-- We join account bounds to the global date spine and keep only months in-range.
account_month_spine as (

    select
        b.account_id,
        d.date_month,
        b.first_active_month,
        b.last_active_month

    from account_bounds as b
    inner join {{ ref('int_date_spine') }} as d
        on d.date_month between b.first_active_month and b.last_active_month

),

-- Step 4 — Gap-fill missing months with zero ARR.
-- Real months keep their aggregated ARR; absent months become explicit 0.
filled as (

    select
        s.account_id,
        s.date_month,
        coalesce(m.monthly_arr, 0.0) as monthly_arr,
        s.first_active_month

    from account_month_spine as s
    left join monthly_arr as m
        on  m.account_id = s.account_id
        and m.date_month = s.date_month

),

-- Step 5 — Previous month ARR over full gap-filled timeline.
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
            0.0
        ) as previous_month_arr

    from filled

)

select * from with_lag
