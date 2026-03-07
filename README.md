# Subscription Revenue Modelling (dbt)

## Overview

Analytical modelling of **Annual Recurring Revenue (ARR)** for a subscription-based business, built with [dbt](https://www.getdbt.com/) and [DuckDB](https://duckdb.org/) for fully local, reproducible execution.

**Stack:** Python 3.13+ (for virtual environment) · dbt-duckdb · SQL · DuckDB

## Problem Statement

Subscription revenue introduces complexities that transactional data does not: time-based allocation, recurring charges, upgrades, downgrades, churn, and reactivations. Traditional reporting often fails to capture these dynamics accurately.

This project models ARR at a **monthly grain**, producing a clean analytical dataset that categorizes month-over-month revenue changes per account. The output supports dashboards, trend analysis, and business reviews around subscription health.

## What This Project Does

- Ingests raw subscription data (accounts, products, start/end dates, ARR)
- Cleans and validates data quality issues at the staging layer
- Generates a monthly date spine and expands subscriptions to active months
- Aggregates ARR per account per month
- Classifies each month's revenue change as: **New**, **Upgrade**, **Downgrade**, **Churn**, **Reactivation**, or **No-change**
- Produces a final fact table (`fct_monthly_arr`) ready for analytical consumption

## Architecture

```
seeds/raw_subscriptions.csv
    |
    v
staging/stg_subscriptions          -- clean, typed, validated
    |
    v
intermediate/int_date_spine        -- monthly calendar spine
intermediate/int_subscription_months  -- 1 row per subscription x active month
intermediate/int_account_monthly_arr  -- aggregated ARR per account x month
    |
    v
marts/fct_monthly_arr              -- final fact table with change categories
```

## Tech Stack

| Component | Tool |
|-----------|------|
| Transformation | dbt-core |
| Warehouse | DuckDB (local) |
| Packages | dbt_utils, codegen |
| Language | SQL + Python (visualization) |

## Quick Start

> Ideally you should have UV installed. See: https://docs.astral.sh/uv/getting-started/installation/

```bash
# Clone and install dependencies
git clone https://github.com/Gerardo1909/subscription-revenue-modelling-dbt> && cd subscription-revenue-modelling-dbt
uv sync

# Configure your local profile (not committed to git)
# Create arr_modeling/profiles.yml pointing to subs_data.duckdb

# Install dbt packages and build
cd arr_modeling
uv run dbt deps
uv run dbt build
```

## Project Structure

```
arr_modeling/
  models/
    staging/           -- data cleaning and validation
    intermediate/      -- date spine, subscription expansion, aggregation
    marts/             -- final analytical models
  seeds/               -- raw subscription data (CSV)
  macros/              -- reusable SQL macros
  tests/               -- singular data tests
```

## Key Design Decisions

- **End date is inclusive**: a subscription with `end_date = Sep 8` is considered active on September 8th
- **Free subscriptions**: ARR values near zero (`1e-9`) are mapped to `$0.00` in staging
- **Corrupt data quarantine**: records with `start_date > end_date` are routed to a quarantine table for review, not silently corrected
- **Local-first**: runs entirely on DuckDB with no cloud dependencies

## Contribution & License

**Author:** Gerardo Toboso  
**Contact:** gerardotoboso1909@gmail.com  
**License:** MIT License
