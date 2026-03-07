{{
    config(
        severity='warn'
    )
}}

-- Singular test: ensures no subscription has a negative ARR value.
--
-- Negative ARR is a data quality error — it has no valid business meaning.
-- Currently passes (0 failures) on the existing dataset, but bootstraps the
-- audit table so that future ingestion of negative ARR records is automatically
-- caught and routed to the quarantine model for ingestion team review
-- (see stg_subscriptions_quarantine).
--
-- Severity is set to 'warn' (not 'error') because the bad records are already
-- excluded from stg_subscriptions via a WHERE filter. This test is purely an
-- audit signal — it should not block downstream models from building.
--
-- We reference the raw seed directly so that the clean staging model
-- (stg_subscriptions) does not need to be the source of truth for bad records.
-- The staging model filters them out; this test independently audits them.

select
    account_id::integer                as account_id,
    subscription_id::varchar           as subscription_id,
    subscription_quantity::integer     as subscription_quantity,
    subscription_deal_close_date::date as deal_close_date,
    subscription_start_date::date      as start_date,
    subscription_end_date::date        as end_date,
    subscription_product_line::varchar as product_line,
    subscription_status::varchar       as status,
    subscription_arr_usd               as arr_usd

from {{ ref('raw_subscriptions') }}
where subscription_arr_usd < 0
