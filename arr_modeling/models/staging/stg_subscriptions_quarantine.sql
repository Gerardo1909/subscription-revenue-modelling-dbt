{{
    config(
        pre_hook=[
            "CREATE SCHEMA IF NOT EXISTS {{ target.schema }}_audit"
        ]
    )
}}

-- Quarantine view: consolidates subscription records that failed data quality tests.
--
-- Architecture: this view reads from dbt's store_failures audit schema.
-- Each UNION ALL branch corresponds to one singular test (tests/*.sql) that
-- detected invalid records. The test name becomes the quarantine_reason, giving
-- the ingestion team a clear entry point for remediation.
--
-- The UNION ALL branches are built dynamically at compile time: only audit tables
-- that already exist are included. This avoids DuckDB's view-validation-at-creation
-- constraint (which would error if we referenced a non-existent table), and also
-- avoids conflicts with dbt's store_failures mechanism on first build. On the
-- first build after a new singular test is added, store_failures creates its audit
-- table and the quarantine view is updated to include it on the next build.
--
-- The pre_hook ensures the audit schema exists before this view is registered.
--
-- To add a new quarantine rule:
--   1. Write a singular test in tests/ that returns the failing rows
--   2. Add the test name to the quarantine_tests list below
--   3. On the next dbt build, the new branch appears automatically

{% set audit_schema = target.schema ~ '_audit' %}

{% set existing_tables_sql %}
    select table_name
    from information_schema.tables
    where table_schema = '{{ audit_schema }}'
    and table_type = 'BASE TABLE'
{% endset %}

{% if execute %}
    {% set results = run_query(existing_tables_sql) %}
    {% set existing_table_names = results.columns[0].values() | list %}
{% else %}
    {% set existing_table_names = [] %}
{% endif %}

{% set quarantine_tests = [
    'assert_valid_subscription_dates',
    'assert_deal_close_before_start',
    'assert_no_negative_arr'
] %}

{% set available_tests = [] %}
{% for test_name in quarantine_tests %}
    {% if test_name in existing_table_names %}
        {% do available_tests.append(test_name) %}
    {% endif %}
{% endfor %}

{% if available_tests | length == 0 %}

select
    null::varchar  as quarantine_reason,
    null::integer  as account_id,
    null::varchar  as subscription_id,
    null::integer  as subscription_quantity,
    null::date     as deal_close_date,
    null::date     as start_date,
    null::date     as end_date,
    null::varchar  as product_line,
    null::varchar  as status,
    null::double   as arr_usd
where 1=0

{% else %}

{% for test_name in available_tests %}
{% if not loop.first %}
union all
{% endif %}
select
    '{{ test_name }}' as quarantine_reason,
    *
from {{ audit_schema }}.{{ test_name }}
{% endfor %}

{% endif %}
