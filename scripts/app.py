from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path(__file__).parent.parent / "arr_modeling" / "subs_data.duckdb"


@st.cache_data
def load_data() -> pd.DataFrame:
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute("SELECT * FROM main.fct_monthly_arr ORDER BY date_month").df()
    con.close()
    return df


def fill_gaps(df: pd.DataFrame) -> pd.DataFrame:
    """Insert zero rows for months with no data (quarantine-driven gaps)."""
    all_months = pd.date_range(
        df["date_month"].min(), df["date_month"].max(), freq="MS"
    )
    existing = set(df["date_month"].dt.to_period("M"))
    gap_rows = [
        {"date_month": m, "monthly_arr": 0.0}
        for m in all_months
        if m.to_period("M") not in existing
    ]
    if not gap_rows:
        return df
    return (
        pd.concat([df, pd.DataFrame(gap_rows)])
        .sort_values("date_month")
        .reset_index(drop=True)
    )


def fmt_usd(value: float) -> str:
    return f"${value:,.2f}"


st.set_page_config(page_title="ARR Dashboard", layout="wide")
st.title("ARR Subscription Revenue Dashboard")

df = load_data()
chart_df = fill_gaps(df)

latest_month = df["date_month"].max()
latest_row = df[df["date_month"] == latest_month].iloc[0]

current_mrr = latest_row["monthly_arr"]
current_customers = int(df[df["date_month"] == latest_month]["is_active"].sum())

col1, col2 = st.columns(2)
col1.metric("Current MRR", fmt_usd(current_mrr))
col2.metric("Current Customers", current_customers)

st.divider()

st.subheader("Monthly ARR Over Time")

fig = px.line(
    chart_df,
    x="date_month",
    y="monthly_arr",
    labels={"date_month": "Month", "monthly_arr": "ARR (USD)"},
)
fig.update_traces(line_color="#1f77b4")
fig.update_layout(
    xaxis_title="Month",
    yaxis_title="ARR (USD)",
    yaxis_tickprefix="$",
    hovermode="x unified",
)

st.plotly_chart(fig, use_container_width=True)
