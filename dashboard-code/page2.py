import os, re, logging, sys
from datetime import datetime, date, timedelta
import pandas as pd
import streamlit as st
import numpy as np
import ldap3
import clickhouse_connect


# =============================================================
# FIXED VERSION — Empty region stays blank, no auto-display
# =============================================================

def run():

    # ---------------------------------------------------------
    # ENV LOADER
    # ---------------------------------------------------------
    def load_env_file(path=".env"):
        if not os.path.exists(path): 
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): 
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        os.environ[k.strip()] = v.strip().strip('"').strip("'")
        except:
            pass

    load_env_file(os.path.join(os.path.dirname(__file__), ".env"))

    # ---------------------------------------------------------
    # ENV VARS
    # ---------------------------------------------------------
    CLICKHOUSE_HOSTS = [h.strip() for h in os.getenv("CLICKHOUSE_HOSTS","127.0.0.1").split(",")]
    CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT","8123"))
    CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB","IOB1")
    CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER","")
    CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD","")

    AGG_REGION_BRANCH_SUMMARY = os.getenv(
        "AGG_REGION_BRANCH_SUMMARY",
        "IOB1.customer_region_branch_summary3"
    )

    # ---------------------------------------------------------
    # THEME
    # ---------------------------------------------------------
    if "theme" not in st.session_state:
        st.session_state.theme = "light"

    def theme_obj():
        if st.session_state.theme == "dark":
            return {
                "BG": "#0e1117", "CARD": "#161b22",
                "TEXT": "#f8f8f8", "SUB": "#9ba1a6",
                "ACCENT": "#3399ff", "TEMPLATE": "plotly_dark",
                "BAR": "#4a90e2"
            }
        return {
            "BG": "#ffffff", "CARD": "#f8f8f8",
            "TEXT": "#222222", "SUB": "#444444",
            "ACCENT": "#004080", "TEMPLATE": "plotly_white",
            "BAR": "#004080"
        }


    T = theme_obj()

    st.markdown(f"""
        <style>
            body, .stApp {{
                background-color: {T["BG"]} !important;
                color: {T["TEXT"]} !important;
            }}
            [data-testid="stSidebar"], section[data-testid="stSidebar"] {{
                background-color: {T["CARD"]} !important;
            }}
            h1, h2, h3, h4, h5, h6, p, label, span {{
                color: {T["TEXT"]} !important;
            }}
        </style>
    """, unsafe_allow_html=True)

    def apply_chart_theme(fig):
        fig.update_traces(marker_color=T["BAR"])
        fig.update_layout(
            paper_bgcolor=T["BG"],
            plot_bgcolor=T["BG"],
            font=dict(color=T["TEXT"]),
            xaxis=dict(color=T["TEXT"], showgrid=False),
            yaxis=dict(color=T["TEXT"], showgrid=False)
        )
        return fig

    # ---------------------------------------------------------
    # SESSION — LDAP REGION SHOULD ALREADY BE SET
    # ---------------------------------------------------------
    if "region" not in st.session_state:
        st.session_state.region = ""

    if "is_central" not in st.session_state:
        st.session_state.is_central = False

    # ---------------------------------------------------------
    # CLICKHOUSE HELPERS
    # ---------------------------------------------------------
    @st.cache_data(ttl=300)
    def _conn_params():
        return CLICKHOUSE_HOSTS, CLICKHOUSE_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DB

    def qdf(sql: str):
        hosts, port, user, pwd, db = _conn_params()
        for h in hosts:
            try:
                client = clickhouse_connect.get_client(
                    host=h, port=port, username=user,
                    password=pwd, database=db, connect_timeout=5
                )
                df = client.query_df(sql)
                client.close()
                return df
            except:
                continue
        return pd.DataFrame()

    # ---------------------------------------------------------
    # REGION RESTRICTION (FINAL WORKING VERSION)
    # ---------------------------------------------------------
    def sanitize_input(v: str):
        return re.sub(r"[^a-zA-Z0-9_.\- ]", "", str(v or ""))

    def region_condition(base_sql: str, field="REGION_NAME", selected_region_override=None):
        # If override is provided, use it; otherwise use session state
        if selected_region_override is not None:
            region = selected_region_override
            is_central = False  # Override means we're filtering by dropdown selection
        else:
            region = st.session_state.get("region", "")
            is_central = st.session_state.get("is_central", False)

        if is_central:
            return base_sql.strip()

        # If region is empty string, we need to match empty/null values
        if region == "":
            clause = f"(trim({field}) = '' OR {field} IS NULL)"
        else:
            if not region:
                return base_sql.strip()
            safe_region = sanitize_input(region)
            clause = f"lower(trim({field})) = lower('{safe_region}')"

        sql = base_sql.strip().rstrip(";")
        sql_lower = sql.lower()

        # If WHERE exists → use AND
        if " where " in sql_lower:
            return f"{sql} AND {clause}"

        # Else → add WHERE
        return f"{sql} WHERE {clause}"

    # ---------------------------------------------------------
    # SUMMARY QUERY - Keep regions as-is (including empty/null)
    # ---------------------------------------------------------
    def load_region_branch_summary(filter_region=None):
        # Keep REGION_NAME as-is, just trim it
        sql = f"""
            SELECT 
                COALESCE(trim(REGION_NAME), '') AS REGION_NAME,
                trim(BRANCH_NAME) AS BRANCH_NAME,
                sum(AccountsDelta) AS SUCCESSFUL_ACCOUNTS
            FROM {AGG_REGION_BRANCH_SUMMARY}
        """

        # Only apply LDAP filter if not filtering by dropdown selection
        if filter_region is None:
            sql = region_condition(sql)
        else:
            # Filter by the specific selected region (including empty)
            if filter_region == "":
                sql += " WHERE (trim(REGION_NAME) = '' OR REGION_NAME IS NULL)"
            else:
                safe_region = sanitize_input(filter_region)
                sql += f" WHERE lower(trim(REGION_NAME)) = lower('{safe_region}')"

        # Now add GROUP BY after filtering
        sql += """
            GROUP BY REGION_NAME, BRANCH_NAME
            ORDER BY REGION_NAME, SUCCESSFUL_ACCOUNTS DESC
        """

        return qdf(sql)

    # ---------------------------------------------------------
    # HEADER
    # ---------------------------------------------------------
    who = st.session_state.region if st.session_state.region else "Unknown Region"

    st.markdown(
        f"<h2 style='text-align:center;font-weight:800;'>Real Time Insights - TAB BANKING</h2>",
        unsafe_allow_html=True
    )

    # ---------------------------------------------------------
    # MAIN SUMMARY UI
    # ---------------------------------------------------------
    # First load to get region list (LDAP filtered if applicable)
    df_summary_all = load_region_branch_summary()
    
    if df_summary_all.empty:
        st.warning("No data available for this region.")
        return

    # Get unique regions - keep empty strings as they are
    region_list = sorted(df_summary_all["REGION_NAME"].unique())

    st.subheader("Choose Region")
    
    # Don't add extra empty option - just use region_list as is
    selected_region = st.selectbox(
        "Select Region", 
        options=region_list,
        index=None,  # No default selection
        key="region_select_154",
        placeholder="Please select a region..."
    )

    # Show data only when something is selected (including empty string)
    if selected_region is not None:
        # Reload data filtered by selected region
        df_filtered = load_region_branch_summary(filter_region=selected_region)
        
        if not df_filtered.empty:
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Branch Table")
                st.dataframe(
                    df_filtered.rename(columns={"SUCCESSFUL_ACCOUNTS": "TOTAL_ACCOUNTS"}), 
                    use_container_width=True, 
                    hide_index=True
                )

            with col2:
                import plotly.express as px
                st.subheader("Branch Graph")
                fig = px.bar(
                    df_filtered,
                    x="BRANCH_NAME",
                    y="SUCCESSFUL_ACCOUNTS",
                    template=T["TEMPLATE"],
                )
                fig.update_layout(height=420, margin=dict(l=20, r=20, t=40, b=40))
                fig = apply_chart_theme(fig)

                st.plotly_chart(fig, use_container_width=True)
        
            # ---------------------------------------------------------
            # TODAY'S SUCCESSFUL ACCOUNTS — USING branch_daily_accounts
            # ---------------------------------------------------------
            st.markdown("---")
            st.subheader(f"Today's Successful Accounts — {selected_region if selected_region else ''}")

            def load_today_from_branch_daily():
                sql = f"""
                    SELECT 
                        COALESCE(trim(REGION_NAME), '') AS REGION_NAME,
                        trim(BRANCH_NAME) AS BRANCH_NAME,
                        SUM(AccountsDelta) AS SUCCESSFUL_ACCOUNTS
                    FROM IOB1.branch_daily_accounts
                    WHERE TXN_DT = today()
                """

                # Filter by the selected region (including empty)
                if selected_region == "":
                    sql += " AND (trim(REGION_NAME) = '' OR REGION_NAME IS NULL) "
                else:
                    safe_region = sanitize_input(selected_region)
                    sql += f" AND lower(trim(REGION_NAME)) = lower('{safe_region}') "

                sql += """
                    GROUP BY REGION_NAME, BRANCH_NAME
                    ORDER BY SUCCESSFUL_ACCOUNTS DESC
                """

                return qdf(sql)

            df_today = load_today_from_branch_daily()

            # Normalize column names
            if not df_today.empty:
                df_today.columns = [c.upper() for c in df_today.columns]

            # ---------------------------------------------------------
            # STRONG NORMALIZATION FUNCTION (fix for hidden characters)
            # ---------------------------------------------------------
            import unicodedata
            def normalize_str(x):
                if x is None or pd.isna(x):
                    return ""
                x = unicodedata.normalize("NFKC", str(x))
                x = x.replace(".", "").replace("-", "")
                x = " ".join(x.split())
                x = x.replace(" ", "")
                return x.lower().strip()

            norm_selected = normalize_str(selected_region)

            # Match today's data with selected region using normalization
            if not df_today.empty and "REGION_NAME" in df_today.columns:
                df_today_region = df_today[
                    df_today["REGION_NAME"].apply(normalize_str) == norm_selected
                ]
            else:
                df_today_region = pd.DataFrame()

            if df_today_region.empty:
                st.info("No successful accounts created today for this region.")
            else:
                t1, t2 = st.columns(2)

                with t1:
                    st.subheader("Today's Accounts - Table")
                    st.dataframe(
                        df_today_region.rename(columns={"SUCCESSFUL_ACCOUNTS": "TODAY_ACCOUNTS"}), 
                        use_container_width=True, 
                        hide_index=True
                    )

                with t2:
                    import plotly.express as px
                    st.subheader("Today's Accounts - Graph")
                    fig_today = px.bar(
                        df_today_region,
                        x="BRANCH_NAME",
                        y="SUCCESSFUL_ACCOUNTS",
                        title=f"Today's Accounts — {selected_region if selected_region else ''}",
                        template=T["TEMPLATE"]
                    )
                    fig_today.update_layout(height=420, margin=dict(l=20, r=20, t=40, b=40))
                    fig_today = apply_chart_theme(fig_today)
                    st.plotly_chart(fig_today, use_container_width=True)

            st.caption(f"Rendered at {datetime.now().strftime('%H:%M:%S')}")
        else:
            st.warning(f"No data found for the selected region.")
    else:
        # Show a prompt when nothing is selected
        st.info("Please select a region from the dropdown above to view data.")
