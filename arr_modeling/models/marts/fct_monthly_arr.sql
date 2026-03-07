-- Mart model: one row per account × month with ARR change classification.
--
-- Thin classification layer. All data preparation (synthetic churn rows,
-- previous_month_arr via LAG, first_active_month) is handled upstream by
-- int_account_monthly_arr_with_churn.
--
-- Adds:
--   - arr_change      : monthly_arr - previous_month_arr
--   - is_active       : whether the account has ARR > 0 in the month
--   - change_category : business classification of the ARR movement
--
-- change_category logic (evaluated in order):
--   new          → first month the account ever had ARR
--   churn        → arr drops to 0 from a non-zero base (includes synthetic churn rows)
--   reactivation → arr > 0 after a gap (previous_month_arr = 0, not first month)
--   upgrade      → arr increased from a non-zero base
--   downgrade    → arr decreased but remains > 0
--   no_change    → arr unchanged month-over-month

with base as (

    select * from {{ ref('int_account_monthly_arr') }}

),

final as (

    select
        account_id,
        date_month,
        monthly_arr,
        previous_month_arr,
        monthly_arr - previous_month_arr as arr_change,
        monthly_arr > 0                  as is_active,
        case
            when monthly_arr > 0 and date_month = first_active_month
                then 'new'
            when monthly_arr = 0 and previous_month_arr > 0
                then 'churn'
            when monthly_arr > 0 and previous_month_arr = 0
                then 'reactivation'
            when monthly_arr > previous_month_arr
                then 'upgrade'
            when monthly_arr < previous_month_arr
                then 'downgrade'
            else 'no_change'
        end                              as change_category

    from base

)

select * from final
