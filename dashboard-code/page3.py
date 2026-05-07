
import os, re
from datetime import datetime, date
import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
from security import global_session_guard

# ================= LOAD ENV ======================
def load_env_file(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k,v=line.split("=",1)
                os.environ[k.strip()]=v.strip().strip("'").strip('"')

load_env_file()

# ================= CLICKHOUSE CONFIG ======================
try:
    import clickhouse_connect
except:
    clickhouse_connect = None

CLICKHOUSE_HOSTS=[h.strip() for h in os.getenv("CLICKHOUSE_HOSTS","127.0.0.1").split(",") if h.strip()]
CLICKHOUSE_PORT=int(os.getenv("CLICKHOUSE_PORT","8123"))
CLICKHOUSE_DB=os.getenv("CLICKHOUSE_DB","IOB1")
CLICKHOUSE_USER=os.getenv("CLICKHOUSE_USER","")
CLICKHOUSE_PASSWORD=os.getenv("CLICKHOUSE_PASSWORD","")
AGG_DEMOGRAPHICS=os.getenv("AGG_DEMOGRAPHICS","IOB.customer_demographics_agg")

# ================= CLICKHOUSE CLIENT WRAPPER ======================
from db_session import get_db_client

def qdf(sql):
    try:
        client = get_db_client()
        df = client.query_df(sql)
        return df
    except:
        return pd.DataFrame()

def apply_dark_mode(fig):
    if st.session_state.get("theme", "light") == "dark":
        fig.update_layout(
            paper_bgcolor="#0E1117",
            plot_bgcolor="#0E1117",
            font_color="white",
            legend=dict(font=dict(color="white")),
        )
        fig.update_xaxes(showgrid=True, gridcolor="#31363F", color="white")
        fig.update_yaxes(showgrid=True, gridcolor="#31363F", color="white")
    return fig



def apply_global_dark_theme():
    if st.session_state.get("theme", "light") == "dark":
        st.markdown("""
        <style>
            /* Apply to all inputs including date picker */
            input[type="date"], input[type="text"], textarea, .stTextInput > div > div > input {
                background-color: #1E1E1E !important;
                color: white !important;
                border: 1px solid #4A4A4A !important;
                border-radius: 6px;
            }

            /* Placeholder text */
            input::placeholder {
                color: #A0A0A0 !important;
            }

            /* Labels */
            label, .css-1p6h08h, .css-17eq0hr, .stMarkdown p {
                color: white !important;
            }

            /* Calendar popup */
            .css-1n76uvr {
                background-color: #1E1E1E !important;
                color: white !important;
            }

            /* Text inside calendar widget */
            .css-1dp5vir, .css-1qg05tj {
                color: white !important;
            }

        </style>
        """, unsafe_allow_html=True)


# ================= REGION FILTER HELPERS ======================
def sanitize(v):
    return re.sub(r"[^a-zA-Z0-9_.\- ]", "", str(v or ""))

user_region = st.session_state.auth_context["region"]
is_central = st.session_state.auth_context["is_central"]

def region_condition(sql, field="REGION_NAME"):
    if is_central:
        return sql
    return f"{sql} AND lower({field}) LIKE lower('%{sanitize(user_region)}%')"

# ================= DATA LOADER ======================
def demographics_data():
    start_date = st.session_state.start_date.strftime("%Y%m%d")
    end_date   = st.session_state.end_date.strftime("%Y%m%d")

    sql = f"""
        SELECT *
        FROM {AGG_DEMOGRAPHICS} FINAL
        WHERE TXN_DT BETWEEN '{start_date}' AND '{end_date}'
    """

    sql = region_condition(sql, field="REGION_NAME")
    return qdf(sql)

# =====================NEWLY ADDED========================
def validate_date_range(from_date: date, to_date: date):

    today = date.today()

    if from_date < MIN_ALLOWED_DATE:
        raise ValueError("Data older than 10 years is not allowed.")

    if from_date > today:
        raise ValueError("From Date cannot be in future.")

    if to_date > today:
        raise ValueError("To Date cannot be in future.")

    if to_date < from_date:
        raise ValueError("To Date cannot be earlier than From Date.")


MAX_YEARS_HISTORY = 10
MIN_ALLOWED_DATE = date.today().replace(year=date.today().year - MAX_YEARS_HISTORY)


# ===================== MAIN RUN FUNCTION ======================
def run():
    # 🔐 SECURITY SESSION CHECK
    if "auth_context" not in st.session_state:
        st.error("Session expired. Please login again.")
        st.stop()
        global_session_guard()

    apply_global_dark_theme()

    st.markdown("## Customer Demographics")


    # Initial default values
    if "start_date" not in st.session_state:
        st.session_state.start_date =  max(
        MIN_ALLOWED_DATE,
        date.today().replace(year=date.today().year - 1)
    )
    if "end_date" not in st.session_state:
        st.session_state.end_date = date.today()

    col1, col2 = st.columns(2)

    with col1:
        start = st.date_input("From Date", value=st.session_state.start_date, min_value=MIN_ALLOWED_DATE, max_value=date.today())

    with col2:
        end = st.date_input("To Date", value=st.session_state.end_date, min_value=MIN_ALLOWED_DATE, max_value=date.today())

    # Update session state & rerun if changed
    if start != st.session_state.start_date or end != st.session_state.end_date:
        st.session_state.start_date = start
        st.session_state.end_date = end
        st.rerun()

    # Prevent user selecting reversed dates
    if st.session_state.start_date > st.session_state.end_date:
        st.warning("⚠ 'From Date' cannot be later than 'To Date'. Please fix the selection.")
        return
    try:
        validate_date_range(
            st.session_state.start_date,
            st.session_state.end_date
        )
    except ValueError as e:
        st.error(f"❌ {e}")
        st.stop()



    df_demo = demographics_data()

    st.markdown(f"### Showing data from **{st.session_state.start_date}** to **{st.session_state.end_date}**")

    if df_demo.empty:
        st.warning("⚠ No records found in this date range.")
        return

    # =============== CHART THEME =================
    chart_theme = "plotly_dark" if st.session_state.get("theme", "light") == "dark" else "plotly_white"
    palette = ['#003366','#004080','#0059b3','#0073e6','#3399ff']
    employment_palette = [
    "#1D416B",  # Dark Navy (Top slice)
    "#2E5B8A",
    "#3F76A8",
    "#4E8BBE",
    "#6AA0CA",
    "#89B8D7",
    "#A9CFE4",
    "#C8E2F0"   # Lightest slice
    ]


    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Income Source")
        income = df_demo.groupby("INCOMESOURCE",as_index=False)["TOTAL_CUSTOMERS"].sum()
        fig = px.bar(income, x="INCOMESOURCE", y="TOTAL_CUSTOMERS", text="TOTAL_CUSTOMERS",
                     color="INCOMESOURCE", color_discrete_sequence=palette, template=chart_theme)
        fig = apply_dark_mode(fig)

        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Employment Status")
        emp = df_demo.groupby("EMPLOYMENTSTATUS",as_index=False)["TOTAL_CUSTOMERS"].sum()
        fig = px.pie(emp, names="EMPLOYMENTSTATUS", values="TOTAL_CUSTOMERS",
                     color="EMPLOYMENTSTATUS", color_discrete_sequence=employment_palette, template=chart_theme)
        fig = apply_dark_mode(fig)

        st.plotly_chart(fig, use_container_width=True)

    with col3:
        st.subheader("Education Level")
        edu = df_demo.groupby("EDUQUALIFICATION",as_index=False)["TOTAL_CUSTOMERS"].sum()
        fig = px.bar(edu, x="EDUQUALIFICATION", y="TOTAL_CUSTOMERS", text="TOTAL_CUSTOMERS",
                     color="EDUQUALIFICATION", color_discrete_sequence=palette, template=chart_theme)
        fig = apply_dark_mode(fig)

        st.plotly_chart(fig, use_container_width=True)

    st.caption(f"Rendered at {datetime.now().strftime('%H:%M:%S')} ✔")
