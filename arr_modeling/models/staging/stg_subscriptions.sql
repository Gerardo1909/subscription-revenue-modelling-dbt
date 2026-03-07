-- Staging model for subscription data.
-- Performs light cleaning and type casting on the raw seed data:
--   - Renames columns to a clean, consistent convention
--   - Casts all fields to their correct data types
--   - Maps the free-subscription sentinel ARR value (~1e-9) to 0.00
--   - Excludes records with inverted start/end dates (routed to quarantine)
--   - Excludes records where deal_close_date > start_date (routed to quarantine)
--   - Excludes records with negative ARR (routed to quarantine)
--
-- Design decisions:
--   - end_date is inclusive: a subscription is considered active on its end_date
--   - Only positive ARR values below $0.001 represent free subscriptions and are zeroed out
--   - Business rule: deal must be closed before or on the subscription start date

with source as (

    select * from {{ ref('raw_subscriptions') }}

),

cleaned as (

    select
        -- identifiers
        account_id::integer                as account_id,
        subscription_id::varchar           as subscription_id,
        subscription_quantity::integer     as subscription_quantity,

        -- dates
        subscription_deal_close_date::date as deal_close_date,
        subscription_start_date::date      as start_date,
        subscription_end_date::date        as end_date,

        -- attributes
        subscription_product_line::varchar as product_line,
        subscription_status::varchar       as status,

        -- revenue
        -- Free subscriptions use a sentinel value of ~1e-9 instead of 0.
        -- We normalize these to 0.00 to avoid floating-point noise downstream.
        -- Only positive values below $0.001 are treated as sentinels; negative
        -- values are excluded upstream by the WHERE filter below.
        case
            when subscription_arr_usd > 0
                 and subscription_arr_usd < 0.001 then 0.0
            else subscription_arr_usd
        end                                as arr_usd

    from source
    -- Records where start_date > end_date are invalid and cannot be corrected
    -- without confirmation from the ingestion team. They are excluded here and
    -- captured in the quarantine model for review.
    where subscription_start_date::date <= subscription_end_date::date
      -- Business rule: the deal must be closed before or on the subscription
      -- start date. Records where deal_close_date > start_date are invalid.
      and subscription_deal_close_date::date <= subscription_start_date::date
      -- Negative ARR is a data quality error; guard for future invalid records.
      and subscription_arr_usd >= 0

)

select * from cleaned
