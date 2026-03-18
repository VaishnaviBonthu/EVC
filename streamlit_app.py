import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Snowflake session
session = get_active_session()

st.set_page_config(layout="wide")
st.title("⚡ EV Charging KPI Dashboard")

# -------------------------------
# 1. Charger Anomaly Score (BAR)
# -------------------------------
st.subheader("🚨 Charger Anomaly Score")

df_anomaly = session.sql("""
SELECT charger_id, anomaly_score
FROM CURATED.KPI_CHARGER_ANOMALY
ORDER BY anomaly_score DESC
LIMIT 20
""").to_pandas()

st.bar_chart(df_anomaly.set_index("CHARGER_ID"))

# -------------------------------
# 2. KPI CARDS (Uptime + Active Users)
# -------------------------------
st.subheader("📌 Key Metrics")

col1, col2 = st.columns(2)

# Charger Uptime Score (Latest Avg)
df_uptime = session.sql("""
SELECT AVG(uptime_score) AS avg_uptime
FROM CURATED.KPI_CHARGER_UPTIME
""").to_pandas()

avg_uptime = df_uptime.iloc[0]["AVG_UPTIME"]

with col1:
    st.metric(label="⚡ Avg Charger Uptime Score", value=f"{avg_uptime:.2f} %")

# Active User Ratio (Latest)
df_users = session.sql("""
SELECT active_user_ratio
FROM CURATED.KPI_ACTIVE_USER_RATIO
ORDER BY kpi_time DESC
LIMIT 1
""").to_pandas()

active_ratio = df_users.iloc[0]["ACTIVE_USER_RATIO"]

with col2:
    st.metric(label="👥 Active User Charging Ratio", value=f"{active_ratio:.2f} %")

# -------------------------------
# 3. Grid Load Distribution Index (BAR)
# -------------------------------
st.subheader("🌍 Grid Load Distribution Index")

df_grid = session.sql("""
SELECT city || '-' || zone AS location, grid_load_index
FROM CURATED.KPI_GRID_LOAD_INDEX
ORDER BY grid_load_index DESC
""").to_pandas()

st.bar_chart(df_grid.set_index("LOCATION"))

# -------------------------------
# 4. Avg Revenue Per Session (LINE)
# -------------------------------
st.subheader("💰 Avg Revenue per Session (Trend by Tariff)")

df_arps = session.sql("""
SELECT kpi_time, tariff_type, avg_revenue_per_session
FROM CURATED.KPI_ARPS
ORDER BY kpi_time
""").to_pandas()

# Pivot for line chart (multiple tariff lines)
df_arps_pivot = df_arps.pivot(
    index="KPI_TIME",
    columns="TARIFF_TYPE",
    values="AVG_REVENUE_PER_SESSION"
)

st.line_chart(df_arps_pivot)

st.success("✅ KPI Dashboard Loaded Successfully!")