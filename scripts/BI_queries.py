"""
Executes BI queries to check on main fact table and quarantine table.

Usage:
    uv run python scripts/BI_queries.py
"""

from pathlib import Path

import duckdb

DB_PATH = Path(__file__).parent.parent / "arr_modeling" / "subs_data.duckdb"


def main() -> None:
    con = duckdb.connect(str(DB_PATH), read_only=True)

    # Query 1: Quarantine reasons count
    print("=" * 80)
    print("QUARANTINE REASONS")
    print("=" * 80)
    df_quarantine = con.execute("""
        SELECT quarantine_reason, count(*) as records
        FROM main.stg_subscriptions_quarantine
        GROUP BY 1
        ORDER BY 2 DESC
    """).df()
    print(df_quarantine.to_string())
    print()

    # Query 2: Monthly ARR
    print("=" * 80)
    print("MONTHLY ARR (FACT TABLE)")
    print("=" * 80)
    df_arr = con.execute("""
        SELECT date_month, monthly_arr, change_category
        FROM main.fct_monthly_arr
        ORDER BY date_month
    """).df()
    print(df_arr.to_string())

    con.close()


if __name__ == "__main__":
    main()
