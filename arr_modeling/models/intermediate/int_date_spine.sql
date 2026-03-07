-- depends_on: {{ ref('stg_subscriptions') }}
-- Monthly date spine covering the full range of subscription data.
--
-- Generates one row per month (first day of month) from the earliest
-- subscription start_date to the latest subscription end_date in stg_subscriptions.
-- The range is computed dynamically from the data so the spine automatically
-- extends as new subscriptions are loaded.
--
-- Implementation note: we use dbt_utils.date_spine (with run_query to obtain
-- dynamic bounds) per the project specification. An execute-guard provides
-- safe fallback literals during dbt's parse phase, while the actual bounds
-- are computed from stg_subscriptions at runtime.

{% if execute %}
    {% set bounds_query %}
        select
            date_trunc('month', min(start_date))::varchar as spine_start,
            date_trunc('month', max(end_date))::varchar   as spine_end
        from {{ ref('stg_subscriptions') }}
    {% endset %}
    {% set results    = run_query(bounds_query) %}
    {% set start_date = results.columns[0].values()[0] %}
    {% set end_date   = results.columns[1].values()[0] %}
{% else %}
    {% set start_date = var('spine_start_fallback') %}
    {% set end_date   = var('spine_end_fallback') %}
{% endif %}

{{ dbt_utils.date_spine(
    datepart='month',
    start_date="cast('" ~ start_date ~ "' as date)",
    end_date="cast('"   ~ end_date   ~ "' as date)"
) }}
