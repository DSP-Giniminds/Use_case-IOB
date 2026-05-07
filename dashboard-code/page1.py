import os
import re
import logging
import sys
import time
import base64
import secrets as _secrets
from datetime import datetime, date, timedelta

import numpy as np
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
import plotly.express as px
import plotly.graph_objects as go

st.set_option("client.showErrorDetails", False)
sys.path.append('/home/misuser/.local/lib/python3.9/site-packages')

from logging.handlers import RotatingFileHandler


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
    except Exception:
        pass


load_env_file(os.path.join(os.path.dirname(__file__), ".env"))

CLICKHOUSE_HOSTS    = [h.strip() for h in os.getenv("CLICKHOUSE_HOSTS", "127.0.0.1").split(",") if h.strip()]
CLICKHOUSE_PORT     = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB       = os.getenv("CLICKHOUSE_DB", "IOB1")
CLICKHOUSE_USER     = os.getenv("CLICKHOUSE_USER", "")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_SSL      = os.getenv("CLICKHOUSE_SSL", "false").lower() == "true"
CLICKHOUSE_VERIFY   = os.getenv("CLICKHOUSE_VERIFY", "true").lower() == "true"
CLICKHOUSE_CA       = os.getenv("CLICKHOUSE_CA", "")
LDAP_SERVER    = os.getenv("LDAP_SERVER",   "iob.in")
LDAP_PORT      = int(os.getenv("LDAP_PORT", "636"))
BASE_DN        = os.getenv("BASE_DN",        "dc=iob,dc=in")
DOMAIN_SUFFIX  = os.getenv("DOMAIN_SUFFIX",  "iob.in")
AUTO_REFRESH_SEC   = int(os.getenv("AUTO_REFRESH_SEC",  "10"))
IDLE_TIMEOUT_MIN   = int(os.getenv("IDLE_TIMEOUT_MIN",  "10"))
MAX_LOGIN_ATTEMPTS = int(os.getenv("MAX_LOGIN_ATTEMPTS", "3"))
AGG_REGION_DAILY = os.getenv("AGG_REGION_DAILY", "IOB1.region_daily_accounts")
AGG_BRANCH_DAILY = os.getenv("AGG_BRANCH_DAILY", "IOB1.branch_daily_accounts")
AGG_SCHM_TREND   = os.getenv("AGG_SCHM_TREND",   "IOB1.schmcode_trend")
AGG_DEMOGRAPHICS = os.getenv("AGG_DEMOGRAPHICS",  "IOB1.customer_demographics3")
EMP_AUTH         = os.getenv("EMP_AUTH",           "IOB1.emp_auth")

LOG_DIR  = os.getenv("LOG_DIR",  "/")
LOG_FILE = os.getenv("LOG_FILE", "log")
try:
    os.makedirs(LOG_DIR, exist_ok=True)
except Exception:
    pass
try:
    handler = RotatingFileHandler(os.path.join(LOG_DIR, LOG_FILE), maxBytes=5_000_000, backupCount=5)
    logging.basicConfig(handlers=[handler], level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    logging.info("===== Dashboard Logging Started =====")
except Exception:
    logging.basicConfig(level=logging.INFO)

try:
    from db_session import get_db_client
except Exception:
    get_db_client = None

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    AES_AVAILABLE = True
except ImportError:
    AES_AVAILABLE = False

_AES_KEY     = _secrets.token_bytes(32)
_AES_IV      = _secrets.token_bytes(12)
_AES_KEY_B64 = base64.b64encode(_AES_KEY).decode()
_AES_IV_B64  = base64.b64encode(_AES_IV).decode()


logging.info("KEY GENERATED ")

def _server_decrypt(ct_b64: str) -> str:
    if not AES_AVAILABLE:
        raise RuntimeError("cryptography library not available")

    logging.error("DECRYPT ATTEMPT")

    ct_b64 = ct_b64.replace("-", "+").replace("_", "/")
    ct_b64 += "=" * ((-len(ct_b64)) % 4)

    try:
        result = AESGCM(_AES_KEY).decrypt(_AES_IV, base64.b64decode(ct_b64), None).decode("utf-8")
        logging.error("DECRYPT SUCCESS")
        return result
    except Exception as e:
        logging.error("DECRYPT INNER FAIL ")
        raise

def _wipe(var):
    return None


try:
    import clickhouse_connect
except Exception:
    clickhouse_connect = None

try:
    from ldap3 import Server, Connection, ALL, SUBTREE, ALL_ATTRIBUTES
except Exception:
    Server = Connection = ALL = SUBTREE = ALL_ATTRIBUTES = None

if "theme" not in st.session_state:
    st.session_state.theme = "light"


def toggle_theme():
    st.session_state.theme = "dark" if st.session_state.theme == "light" else "light"


# ─────────────────────────────────────────────────────────────
# FIX 1: get_theme() is called fresh every time it is needed —
#         never cached in a module-level variable T.
# ─────────────────────────────────────────────────────────────
def get_theme():
    if "theme" not in st.session_state:
        st.session_state.theme = "light"
    if st.session_state.theme == "dark":
        return {
            "MODE": "dark", "BG": "#0e1117", "CARD": "#161b22",
            "TEXT": "#f8f8f8", "SUB": "#9ba1a6", "ACCENT": "#3399ff",
            "OK": "#21bf73", "ERR": "#fa6a6a", "TEMPLATE": "plotly_dark",
        }
    return {
        "MODE": "light", "BG": "#ffffff", "CARD": "#f8f8f8",
        "TEXT": "#222222", "SUB": "#444444", "ACCENT": "#004080",
        "OK": "#21bf73", "ERR": "#fa6a6a", "TEMPLATE": "plotly_white",
    }


DARK_BLUE = ['#003366', '#004080', '#0059b3', '#0073e6', '#3399ff']
BLUE_PAL  = ['#0d6efd', '#0b5ed7', '#0a58ca', '#084298', '#052c65']
employment_palette = ["#1D416B","#2E5B8A","#3F76A8","#4E8BBE","#6AA0CA","#89B8D7","#A9CFE4","#C8E2F0"]


# ─────────────────────────────────────────────────────────────
# FIX 2: apply_theme_to_fig — removed invalid `theme=` kwarg;
#         always reads T fresh from get_theme().
# ─────────────────────────────────────────────────────────────
def apply_theme_to_fig(fig, height=None, x_title=None, y_title=None):
    T = get_theme()
    try:
        fig.update_layout(
            paper_bgcolor=T["BG"],
            plot_bgcolor=T["BG"],
            font=dict(color=T["TEXT"]),
            template=T["TEMPLATE"],
            margin=dict(l=10, r=10, t=30, b=20),
            height=height or 320,
        )
        if x_title:
            fig.update_xaxes(title_text=x_title, title_font=dict(size=11, color=T["SUB"]))
        if y_title:
            fig.update_yaxes(title_text=y_title, title_font=dict(size=11, color=T["SUB"]))
    except Exception:
        pass
    return fig


def sanitize_input(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.\- ]", "", str(value or "").strip())

def is_session_expired():
    if "last_active" not in st.session_state: return False
    return (datetime.now() - st.session_state.last_active).seconds / 60 > IDLE_TIMEOUT_MIN

def update_activity():
    st.session_state.last_active = datetime.now()

def qdf(sql: str) -> pd.DataFrame:
    try:
        client = get_db_client()
        if client is None: return pd.DataFrame()
        return client.query_df(sql.strip())
    except Exception:
        logging.warning("Database query failed")
        return pd.DataFrame()

def region_condition(sql: str, field="REGION_NAME"):
    sql = sql.strip().rstrip(";")
    region = st.session_state.get("region", "")
    is_central = st.session_state.get("is_central", False)
    if is_central or not region: return sql
    clause = f"lower({field}) LIKE lower('%{sanitize_input(region)}%')"
    sql_lower = sql.lower()
    if " where " in sql_lower: return sql + f" AND {clause}"
    m = re.search(r"\bgroup\s+by\b", sql_lower)
    if m:
        idx = m.start()
        return f"{sql[:idx].rstrip()} WHERE {clause} {sql[idx:]}"
    return f"{sql} WHERE {clause}"

def apply_region_multiselect(sql: str, field="REGION_NAME"):
    if not st.session_state.get("is_central", False): return sql
    sel = st.session_state.get("selected_regions")
    if sel:
        safe = [f"'{sanitize_input(x)}'" for x in sel]
        clause = f"{field} IN ({', '.join(safe)})"
        if " where " in sql.lower(): return sql + f" AND {clause}"
        return sql + f" WHERE {clause}"
    return sql

def _to_date(df, col="TXN_DT"):
    if df is not None and not df.empty and col in df.columns:
        try: df[col] = pd.to_datetime(df[col]).dt.date
        except Exception: pass
    return df

def exec_sql(sql: str):
    try:
        client = get_db_client()
        client.command(sql)
    except Exception as e:
        logging.error(f"ClickHouse exec failed: {e}")

def local_service_auth(emp_id, password):
    service_users = {"super_user": ("super_user-secret", "CENTRAL"), "admin": ("admin-secret", "PUNE")}
    if emp_id in service_users:
        real_pwd, region = service_users[emp_id]
        if password == real_pwd: return True, f"R.O.{region}"
    return False, ""

def ldap_authenticate(emp_id, password):
    emp_id = sanitize_input(emp_id)
    ok_local, region_local = local_service_auth(emp_id, password)
    if ok_local: return True, region_local
   
    try:
        if not Server: return False, ""
        server = Server(LDAP_SERVER, port=LDAP_PORT, use_ssl=True, get_info=ALL)
        conn = Connection(server, user=f"{emp_id}@{DOMAIN_SUFFIX}", password=password, auto_bind=True)
        conn.search(BASE_DN, f"(|(uid={emp_id})(sAMAccountName={emp_id})(employeeID={emp_id}))",
                    SUBTREE, attributes=ALL_ATTRIBUTES)
        if not conn.entries: return False, ""
        user_dn = conn.entries[0].entry_dn.upper()
        ou_list = [p.replace("OU=", "").strip() for p in user_dn.split(",") if p.startswith("OU=")]
        region = ou_list[1] if len(ou_list) >= 2 else (ou_list[0] if ou_list else "")
        for w in ["OFFICERS", "OFFICER", "STAFF", "ADMIN", "BRANCH", "TEAM"]: region = region.replace(w, "")
        region = region.strip().replace(" III", "-III").replace(" II", "-II").replace(" I", "-I")
        if not region.startswith("R.O."): region = f"R.O.{region}"
        return True, region.strip().upper()
    except Exception:
        logging.warning("LDAP authentication failed")
        return False, ""

def is_employee_authorized(emp_id):
    try:
        clean = sanitize_input(emp_id)
        df = qdf(f"SELECT COUNT(*) AS cnt FROM {EMP_AUTH} WHERE emp_id = '{clean}' AND is_active = 1")
        if df.empty: return False, "Authorization check failed."
        count = int(df.iloc[0, 0])
        return (count > 0), ("Employee authorized" if count > 0 else "User not allowed")
    except Exception:
        logging.error("Authorization check failed")
        return False, "Authorization check failed."

def get_account_state(emp_id):
    emp_id = sanitize_input(emp_id)
    df = qdf(f"SELECT failed_attempts,locked_until,status FROM DR.login_lock WHERE emp_id='{emp_id}' ORDER BY updated_at DESC LIMIT 1")
    if df.empty: return 0, None, "ACTIVE"
    return int(df.iloc[0]["failed_attempts"]), df.iloc[0]["locked_until"], df.iloc[0]["status"]

def is_login_allowed(emp_id):
    attempts, locked_until, status = get_account_state(emp_id)
    if status == "PERM_LOCKED": return False, "PERM_LOCKED", None, attempts
    return True, "ACTIVE", None, attempts

def register_failed_login(emp_id):
    emp_id = sanitize_input(emp_id)
    attempts, _, status = get_account_state(emp_id)
    if status == "PERM_LOCKED": return
    attempts += 1
    lock_status = "PERM_LOCKED" if attempts >= 3 else "ACTIVE"
    exec_sql(f"INSERT INTO DR.login_lock VALUES ('{emp_id}',{attempts},NULL,'{lock_status}',now())")
    if lock_status == "PERM_LOCKED": logging.warning(f"User permanently locked: {emp_id}")


# =============================================================
# LOGIN
# =============================================================
def run_login_check():
    # ── Always read theme fresh ──────────────────────────────
    T = get_theme()

    if "authenticated" not in st.session_state:
        st.session_state.authenticated    = False
        st.session_state.region           = ""
        st.session_state.is_central       = False
        st.session_state.login_attempts   = 0
        st.session_state.last_active      = datetime.now()
        st.session_state.selected_regions = []
        st.session_state.logout_trigger   = False

    if st.session_state.get("authenticated", False):
        return True

    # ── Read encrypted credentials from query params ─────────────────────
    params = st.query_params
    qp_eid = params.get("_eid", "")
    qp_pwd = params.get("_pwd", "")

    if qp_eid and qp_pwd:
        st.query_params.clear()
        try:
            emp_id_plain   = _server_decrypt(qp_eid)
            password_plain = _server_decrypt(qp_pwd)
        except Exception as e:
            import traceback
            logging.error(f"""
        === DECRYPTION FAILURE ===
        Error Type : {type(e).__name__}
        Error Msg  : {str(e)}
        PID        : {os.getpid()}
        Session Key: {base64.b64encode(st.session_state.get('_AES_KEY', b'')).decode()[:8] if st.session_state.get('_AES_KEY') else 'NOT IN SESSION'}
        Module Key : {_AES_KEY_B64[:8]}
        Key Match  : {st.session_state.get('_AES_KEY') == _AES_KEY}
        qp_eid len : {len(qp_eid)}
        qp_pwd len : {len(qp_pwd)}
        Traceback  :
        {traceback.format_exc()}
        ==========================
        """)
            st.session_state["_login_error"] = "Security error. Please try again."
            st.rerun()
        emp_id       = sanitize_input(emp_id_plain)
        emp_id_plain = None

        if not emp_id or not password_plain:
            password_plain = _wipe(password_plain)
            st.session_state["_login_error"] = "Empty credentials."
            st.rerun()

        allowed, status, _, attempts = is_login_allowed(emp_id)
        attempts = int(attempts or 0)
        if not allowed and status == "PERM_LOCKED":
            password_plain = _wipe(password_plain)
            st.session_state["_login_error"] = "Account permanently locked. Contact administrator."
            st.rerun()

        is_authorized, _ = is_employee_authorized(emp_id)
        if not is_authorized:
            password_plain = _wipe(password_plain)
            register_failed_login(emp_id)
            st.session_state["_login_error"] = "Access denied. User not authorized."
            st.rerun()

        with st.spinner("Authenticating..."):
            ok, region = ldap_authenticate(emp_id, password_plain)
        password_plain = _wipe(password_plain)

        if not ok:
            register_failed_login(emp_id)
            next_attempt = attempts + 1
            if next_attempt >= MAX_LOGIN_ATTEMPTS:
                st.session_state["_login_error"] = "Account permanently locked. Contact administrator."
                st.rerun()
            remaining = max(0, MAX_LOGIN_ATTEMPTS - next_attempt)
            st.session_state["_login_error"] = f"Invalid credentials. Attempts left: {remaining}"
            st.rerun()

        import secrets as _sec
        exec_sql(f"INSERT INTO IOB1.login_lock VALUES ('{emp_id}',0,NULL,'ACTIVE',now())")
        st.session_state.authenticated = True
        st.session_state.emp_id        = emp_id
        st.session_state.region        = region or ""
        st.session_state.is_central    = "CENTRAL" in (region or "").upper()
        st.session_state.auth_context  = {
            "emp_id": emp_id, "region": region or "",
            "is_central": "CENTRAL" in (region or "").upper(),
            "login_time": datetime.now(),
        }
        st.session_state.session_token = _sec.token_urlsafe(32)
        update_activity()
        logging.info(f"Login successful: {emp_id}")
        st.rerun()

    st.markdown(f"""
    <style>
        header, footer, #MainMenu {{ visibility: hidden !important; }}
        html, body, .stApp        {{ background-color: {T["BG"]} !important; }}
        .block-container          {{ padding-top: 30px !important; max-width: 460px !important; }}
    </style>
    """, unsafe_allow_html=True)

    try:
        st.image("/mnt/test/banklogo.png", width=260)
    except Exception:
        pass

    st.markdown(f"""
    <div style="font-size:24px;font-weight:800;text-align:center;
                margin:8px 0 20px;color:#003366;">
        Real-Time MIS Dashboard
    </div>
    """, unsafe_allow_html=True)

    err_msg = st.session_state.pop("_login_error", None)
    if err_msg:
        st.error(err_msg)

    components.html(f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
*{{box-sizing:border-box;margin:0;padding:0;font-family:Arial,sans-serif}}
body{{background:transparent;padding:2px 0}}
.row{{margin-bottom:10px}}
input{{width:100%;padding:10px 12px;border:1.5px solid #c8d0dc;border-radius:6px;
    font-size:14px;outline:none;background:#fff;color:#222}}
input:focus{{border-color:#004080;box-shadow:0 0 0 3px rgba(0,64,128,.12)}}
#btn{{width:100%;padding:12px;background:#004080;color:#fff;border:none;
    border-radius:6px;font-size:15px;font-weight:700;cursor:pointer;margin-top:2px}}
#btn:hover{{background:#002d59}}
#btn:disabled{{background:#7a9bbf;cursor:not-allowed}}
#msg{{margin-top:8px;font-size:13px;min-height:18px;text-align:center;color:#004080}}
#login-link{{
    display:none;
    width:100%;padding:12px;background:#1b5e20;color:#fff;
    border-radius:6px;font-size:15px;font-weight:700;
    text-align:center;text-decoration:none;margin-top:8px;
}}
#login-link:hover{{background:#145214}}
</style>
</head>
<body>
<div class="row">
  <input type="text" id="eid" placeholder="Enter Employee ID"
         autocomplete="username" spellcheck="false"/>
</div>
<div class="row">
  <input type="password" id="pwd" placeholder="Enter Password"
         autocomplete="current-password"/>
</div>
<button id="btn" onclick="doLogin()">Login</button>
<a id="login-link" href="#" target="_top">Click here to complete login</a>
<div id="msg"></div>

<script>
const KEY64 = "{_AES_KEY_B64}";
const IV64  = "{_AES_IV_B64}";
let _key = null;

function b64ToBytes(b64) {{
    return Uint8Array.from(atob(b64), c => c.charCodeAt(0));
}}
async function getKey() {{
    if (_key) return _key;
    _key = await crypto.subtle.importKey(
        "raw", b64ToBytes(KEY64), {{name:"AES-GCM"}}, false, ["encrypt"]);
    return _key;
}}
async function aesEncrypt(text) {{
    const key = await getKey();
    const ct = await crypto.subtle.encrypt(
        {{name:"AES-GCM", iv:b64ToBytes(IV64)}}, key, new TextEncoder().encode(text));
    return btoa(String.fromCharCode(...new Uint8Array(ct)))
        .replace(/\+/g,'-').replace(/\//g,'_').replace(/=/g,'');
}}

async function doLogin() {{
    const eid = document.getElementById('eid').value.trim();
    const pwd = document.getElementById('pwd').value;
    const btn = document.getElementById('btn');
    const msg = document.getElementById('msg');
    const link = document.getElementById('login-link');

    if (!eid) {{ msg.textContent='Please enter Employee ID.'; msg.style.color='red'; return; }}
    if (!pwd) {{ msg.textContent='Please enter Password.';   msg.style.color='red'; return; }}

    btn.disabled = true;
    btn.textContent = 'Encrypting...';
    msg.textContent = 'Encrypting credentials...';
    msg.style.color = '#004080';

    try {{
        const ctEid = await aesEncrypt(eid);
        const ctPwd = await aesEncrypt(pwd);

        document.getElementById('eid').value = '';
        document.getElementById('pwd').value = '';

        const url = new URL(window.parent.location.href);
        url.searchParams.set('_eid', ctEid);
        url.searchParams.set('_pwd', ctPwd);
        const loginUrl = url.toString();

        link.href = loginUrl;
        link.style.display = 'block';
        btn.style.display = 'none';

        msg.textContent = 'Ready! Click the green button above to log in.';
        msg.style.color = '#1b5e20';

        try {{ link.click(); }} catch(e) {{ }}

    }} catch(err) {{
        msg.textContent = 'Error: ' + err.message;
        msg.style.color = 'red';
        btn.disabled = false;
        btn.textContent = 'Login';
        console.error(err);
    }}
}}

document.addEventListener('keydown', e => {{ if (e.key==='Enter') doLogin(); }});
</script>
</body>
</html>
""", height=260, scrolling=False)

    return False


# =============================================================
# MAIN DASHBOARD
# =============================================================
def run():
    # ── FIX 3: Always read T fresh at the top of run() ──────
    T = get_theme()

    if not st.session_state.get("authenticated", False):
        st.warning("Please login to continue.")
        return

    if is_session_expired():
        st.warning("Session expired. Please log in again.")
        for k in list(st.session_state.keys()):
            del st.session_state[k]
        st.rerun()

    update_activity()

    # ── FIX 4: Inject per-render theme CSS so cards / text
    #           always match the current mode ─────────────────
    st.markdown(f"""
    <style>
        /* App background */
        .stApp, html, body {{
            background-color: {T["BG"]} !important;
            color: {T["TEXT"]} !important;
        }}
        /* All text elements */
        h1,h2,h3,h4,h5,h6,p,span,label,div,caption {{
            color: {T["TEXT"]} !important;
        }}
        /* Sidebar */
        section[data-testid="stSidebar"] {{
            background-color: {T["CARD"]} !important;
        }}
        /* Metric widgets */
        div[data-testid="stMetricValue"],
        div[data-testid="stMetricLabel"],
        div[data-testid="stMetricDelta"] {{
            color: {T["TEXT"]} !important;
        }}
        /* Plotly chart containers */
        .js-plotly-plot .plotly,
        .plot-container.plotly {{
            background-color: {T["BG"]} !important;
        }}
        /* Dividers */
        hr {{ border-color: {T["SUB"]} !important; opacity: 0.3; }}
        /* Caption */
        .stMarkdown small, .stCaption {{
            color: {T["SUB"]} !important;
        }}
    </style>
    """, unsafe_allow_html=True)

    st.markdown("<h1 style='text-align:center;font-weight:800;'>Real Time Insights - TAB BANKING</h1>",
                unsafe_allow_html=True)

    def total_accounts():
        sql = apply_region_multiselect(region_condition(f"SELECT sum(AccountsDelta) AS s FROM {AGG_REGION_DAILY}"))
        df = qdf(sql)
        return int(df.fillna(0).iloc[0, 0] or 0) if not df.empty else 0

    def today_accounts():
        sql = apply_region_multiselect(region_condition(
            f"SELECT sum(AccountsDelta) AS s FROM {AGG_REGION_DAILY} WHERE TXN_DT=today()"))
        df = qdf(sql)
        return int(df.fillna(0).iloc[0, 0] or 0) if not df.empty else 0

    def region_data():
        sql = f"SELECT REGION_NAME,TXN_DT,sum(AccountsDelta) AS AccountsCreated FROM {AGG_REGION_DAILY} GROUP BY REGION_NAME,TXN_DT"
        return _to_date(qdf(apply_region_multiselect(region_condition(sql))), "TXN_DT")

    def branch_data():
        sql = f"SELECT BRANCH_NAME,REGION_NAME,TXN_DT,sum(AccountsDelta) AS AccountsCreated FROM {AGG_BRANCH_DAILY} GROUP BY BRANCH_NAME,REGION_NAME,TXN_DT"
        return _to_date(qdf(apply_region_multiselect(region_condition(sql))), "TXN_DT")

    def schm_data():
        sql = f"SELECT SCHMCODE,REGION_NAME,TXN_DT,sum(AccountsDelta) AS AccountsCreated FROM {AGG_SCHM_TREND} GROUP BY SCHMCODE,REGION_NAME,TXN_DT"
        sql = apply_region_multiselect(region_condition(sql, field="REGION_NAME"), field="REGION_NAME")
        try: return _to_date(qdf(sql), "TXN_DT")
        except: return pd.DataFrame()

    def demographics_data():
        sql = f"""SELECT REGION_NAME,INCOMESOURCE,EMPLOYMENTSTATUS,EDUQUALIFICATION,
                   TOTALINCOME_CLEAN,TOTAL_CUSTOMERS,AVG_INCOME,MEDIAN_INCOME,STDDEV_INCOME
            FROM {AGG_DEMOGRAPHICS}"""
        sql = apply_region_multiselect(region_condition(sql, field="REGION_NAME"), field="REGION_NAME")
        return qdf(sql)

    placeholder = st.empty()
    iteration   = 0

    try:
        while True:
            if st.session_state.get("theme_refresh", False):
                placeholder.empty()
                st.session_state["theme_refresh"] = False
                st.stop()
            if st.session_state.get("current_page") != "Main Dashboard":
                st.stop()
                return
            iteration += 1
            if st.session_state.get("logout_trigger", False):
                st.session_state.logout_trigger = False
                return

            today = date.today()

            # ── FIX 5: Re-read T on every loop iteration ─────
            T = get_theme()

            with placeholder.container():
                update_activity()
                try:
                    df_r = region_data()
                    df_b = branch_data()
                except Exception:
                    df_r = df_b = pd.DataFrame()

                k_total  = total_accounts()
                k_today  = today_accounts()

                k_regions = 0
                if not df_r.empty and "TXN_DT" in df_r.columns and "AccountsCreated" in df_r.columns:
                    rt = df_r[df_r["TXN_DT"] == today]
                    k_regions = rt[rt["AccountsCreated"] > 0]["REGION_NAME"].nunique()

                k_branches = 0
                if not df_b.empty and "TXN_DT" in df_b.columns and "AccountsCreated" in df_b.columns:
                    bt = df_b[df_b["TXN_DT"] == today]
                    k_branches = bt[bt["AccountsCreated"] > 0]["BRANCH_NAME"].nunique()

                # ── FIX 6: KPI cards use live T values ───────
                st.markdown(f"""
                <div style="display:flex;flex-wrap:wrap;gap:12px;margin:8px 0 12px;justify-content:center;">
                    <div style="flex:1 1 180px;background:{T['CARD']};padding:10px;border-radius:8px;
                                text-align:center;border:1px solid {'#2a2f3a' if T['MODE']=='dark' else '#e0e0e0'};">
                        <div style="font-size:12px;color:{T['SUB']}">Total Accounts</div>
                        <div style="font-size:20px;font-weight:700;color:{T['ACCENT']}">{k_total:,}</div>
                    </div>
                    <div style="flex:1 1 180px;background:{T['CARD']};padding:10px;border-radius:8px;
                                text-align:center;border:1px solid {'#2a2f3a' if T['MODE']=='dark' else '#e0e0e0'};">
                        <div style="font-size:12px;color:{T['SUB']}">Today's Accounts</div>
                        <div style="font-size:20px;font-weight:700;color:{T['ACCENT']}">{k_today:,}</div>
                    </div>
                    <div style="flex:1 1 180px;background:{T['CARD']};padding:10px;border-radius:8px;
                                text-align:center;border:1px solid {'#2a2f3a' if T['MODE']=='dark' else '#e0e0e0'};">
                        <div style="font-size:12px;color:{T['SUB']}">Active Regions</div>
                        <div style="font-size:20px;font-weight:700;color:{T['ACCENT']}">{k_regions:,}</div>
                    </div>
                    <div style="flex:1 1 180px;background:{T['CARD']};padding:10px;border-radius:8px;
                                text-align:center;border:1px solid {'#2a2f3a' if T['MODE']=='dark' else '#e0e0e0'};">
                        <div style="font-size:12px;color:{T['SUB']}">Active Branches</div>
                        <div style="font-size:20px;font-weight:700;color:{T['ACCENT']}">{k_branches:,}</div>
                    </div>
                </div>""", unsafe_allow_html=True)

                if st.session_state.get("is_central", False):
                    st.markdown("### Region Leaderboards")
                    if not df_r.empty and "REGION_NAME" in df_r.columns:
                        df_today = df_r[df_r["TXN_DT"] == today].copy()
                        df_today["REGION_NAME"] = (df_today["REGION_NAME"]
                            .fillna("UNKNOWN REGION").replace("", "UNKNOWN REGION").replace(" ", "UNKNOWN REGION"))
                        region_totals_today = (df_today.groupby("REGION_NAME")["AccountsCreated"]
                            .sum().reset_index().sort_values("AccountsCreated", ascending=False))
                        top5    = region_totals_today.head(5).sort_values("AccountsCreated", ascending=True)
                        bottom5 = region_totals_today.tail(5)
                        last_7  = [today - timedelta(days=i) for i in range(6, -1, -1)]
                        try:
                            daily_7 = (df_r[df_r["TXN_DT"].isin(last_7)]
                                .groupby("TXN_DT")["AccountsCreated"].sum().reindex(last_7, fill_value=0))
                        except Exception:
                            daily_7 = pd.Series([0] * 7, index=last_7)
                        avg_7     = float(daily_7.mean()) if len(daily_7) else 0.0
                        today_val = int(daily_7.iloc[-1]) if len(daily_7) else 0

                        col1, col2, col3 = st.columns(3, gap="large")
                        with col1:
                            st.subheader("Top 5 Regions (Today)")
                            fig_top = px.bar(top5, x="AccountsCreated", y="REGION_NAME",
                                             orientation="h", text="REGION_NAME",
                                             color_discrete_sequence=[DARK_BLUE[3]])
                            fig_top.update_traces(texttemplate="%{text}", textposition="inside",
                                                  insidetextanchor="middle", textfont_color="white")
                            fig_top.update_yaxes(showticklabels=False)
                            st.plotly_chart(apply_theme_to_fig(fig_top, height=350),
                                            use_container_width=True, key=f"top_regions_{iteration}")
                        with col2:
                            st.subheader("Bottom 5 Regions (Today)")
                            fig_bottom = px.bar(bottom5, x="AccountsCreated", y="REGION_NAME",
                                                orientation="h", text="REGION_NAME",
                                                color_discrete_sequence=["#fa6a6a"])
                            fig_bottom.update_traces(texttemplate="%{text}", textposition="inside",
                                                     insidetextanchor="middle", textfont_color="white")
                            fig_bottom.update_yaxes(showticklabels=False)
                            st.plotly_chart(apply_theme_to_fig(fig_bottom, height=350),
                                            use_container_width=True, key=f"bottom_regions_{iteration}")
                        with col3:
                            st.subheader("Today vs 7-Day Avg")
                            fig_gauge = go.Figure(go.Indicator(
                                mode="gauge+number+delta", value=today_val,
                                delta={"reference": avg_7},
                                gauge={"axis": {"range": [0, max(8000, avg_7, today_val)]},
                                       "bar": {"color": DARK_BLUE[2]},
                                       "bgcolor": T["BG"],
                                       "threshold": {"line": {"color": "red", "width": 4},
                                                     "thickness": 0.75, "value": avg_7}}))
                            st.plotly_chart(apply_theme_to_fig(fig_gauge, height=350),
                                            use_container_width=True, key=f"region_gauge_{iteration}")

                st.markdown("### Branch Leaderboards")
                if not df_b.empty and "BRANCH_NAME" in df_b.columns:
                    df_b_today = df_b[df_b["TXN_DT"] == today].copy()
                    df_b_today["BRANCH_NAME"] = (df_b_today["BRANCH_NAME"]
                        .fillna("UNKNOWN BRANCH").replace("", "UNKNOWN BRANCH").replace(" ", "UNKNOWN BRANCH"))
                    if df_b_today.empty:
                        st.warning("No branch transactions for today.")
                        df_b_today = pd.DataFrame(columns=["BRANCH_NAME", "AccountsCreated"])
                    branch_totals_today = (df_b_today.groupby("BRANCH_NAME")["AccountsCreated"]
                        .sum().reset_index().sort_values("AccountsCreated", ascending=False))
                    top_branches    = branch_totals_today.head(5).sort_values("AccountsCreated", ascending=True)
                    bottom_branches = branch_totals_today.tail(5)
                    last_30_date = date.today() - timedelta(days=29)
                    full_index   = pd.date_range(last_30_date, date.today(), freq="D").date
                    try:
                        daily_branch = df_b.groupby("TXN_DT")["AccountsCreated"].sum().reindex(full_index, fill_value=0)
                    except Exception:
                        daily_branch = pd.Series(0, index=full_index)
                    seven_sum  = int(daily_branch[-7:].sum()) if len(daily_branch) >= 7 else int(daily_branch.sum())
                    thirty_avg = float(daily_branch.mean()) if len(daily_branch) else 0.0

                    b1, b2, b3 = st.columns(3, gap="large")
                    with b1:
                        st.subheader("Top 5 Branches (Today)")
                        fig_top_b = px.bar(top_branches, x="AccountsCreated", y="BRANCH_NAME",
                                           orientation="h", text="BRANCH_NAME",
                                           color_discrete_sequence=[DARK_BLUE[3]])
                        fig_top_b.update_traces(texttemplate="%{text}", textposition="inside",
                                                insidetextanchor="middle", textfont_color="white")
                        fig_top_b.update_yaxes(showticklabels=False)
                        st.plotly_chart(apply_theme_to_fig(fig_top_b, height=350),
                                        use_container_width=True, key=f"top_branches_{iteration}")
                    with b2:
                        st.subheader("Bottom 5 Branches (Today)")
                        fig_bottom_b = px.bar(bottom_branches, x="AccountsCreated", y="BRANCH_NAME",
                                              orientation="h", text="BRANCH_NAME",
                                              color_discrete_sequence=["#fa6a6a"])
                        fig_bottom_b.update_traces(texttemplate="%{text}", textposition="inside",
                                                   insidetextanchor="middle", textfont_color="white")
                        fig_bottom_b.update_yaxes(showticklabels=False)
                        st.plotly_chart(apply_theme_to_fig(fig_bottom_b, height=350),
                                        use_container_width=True, key=f"bottom_branches_{iteration}")
                    with b3:
                        st.subheader("7-Day vs 30-Day Avg")
                        max_val = max(seven_sum, thirty_avg * 1.5, 10)
                        fig_gauge_b = go.Figure(go.Indicator(
                            mode="gauge+number+delta", value=seven_sum,
                            delta={"reference": thirty_avg,
                                   "increasing": {"color": "#21bf73"},
                                   "decreasing": {"color": "#d9534f"}},
                            gauge={"axis": {"range": [0, max_val]},
                                   "bar": {"color": DARK_BLUE[3]},
                                   "bgcolor": T["BG"],
                                   "threshold": {"line": {"color": "red", "width": 4},
                                                 "thickness": 0.75, "value": thirty_avg}}))
                        st.plotly_chart(apply_theme_to_fig(fig_gauge_b, height=350),
                                        use_container_width=True, key=f"branch_gauge_{iteration}")
                else:
                    st.info("No branch data available.")

                st.markdown("### Account Creation Trends")
                t1, t2, t3 = st.columns(3)
                with t1:
                    st.subheader("7-Day Trend")
                    df_7 = df_r.copy() if not df_r.empty else pd.DataFrame()
                    if not df_7.empty and "TXN_DT" in df_7.columns:
                        last_7_mask = pd.to_datetime(df_7["TXN_DT"]).dt.date >= (date.today() - timedelta(days=6))
                        df_7d = (df_7[last_7_mask]
                            .groupby("TXN_DT", as_index=False)["AccountsCreated"].sum()
                            .sort_values("TXN_DT"))
                        df_7d["RollingAvg"] = df_7d["AccountsCreated"].rolling(window=7, min_periods=1).mean()
                        fig_line = go.Figure()
                        fig_line.add_trace(go.Scatter(x=df_7d["TXN_DT"], y=df_7d["AccountsCreated"],
                                                      mode="lines+markers", name="Daily Accounts"))
                        fig_line.add_trace(go.Scatter(x=df_7d["TXN_DT"], y=df_7d["RollingAvg"],
                                                      mode="lines", name="7-Day Rolling Avg"))
                        st.plotly_chart(apply_theme_to_fig(fig_line, height=320, x_title="Date", y_title="Accounts"),
                                        use_container_width=True, key=f"trend_7day_{iteration}")
                    else:
                        st.info("Not enough data for 7-day trend.")
                with t2:
                    st.subheader("Last 6 Months")
                    df_region_all = df_r.copy() if not df_r.empty else pd.DataFrame()
                    if not df_region_all.empty and "TXN_DT" in df_region_all.columns:
                        start_month = (date.today().replace(day=1) - pd.DateOffset(months=5)).date()
                        df_m = df_region_all[pd.to_datetime(df_region_all["TXN_DT"]).dt.date >= start_month].copy()
                        if not df_m.empty:
                            df_m["Month"] = pd.to_datetime(df_m["TXN_DT"]).dt.to_period("M").astype(str)
                            month_counts  = df_m.groupby("Month")["AccountsCreated"].sum().reset_index()
                            fig_bar_m     = px.bar(month_counts, x="Month", y="AccountsCreated", text="AccountsCreated")
                            st.plotly_chart(apply_theme_to_fig(fig_bar_m, height=320, x_title="Month", y_title="Accounts"),
                                            use_container_width=True, key=f"trend_6months_{iteration}")
                        else:
                            st.info("No 6-month aggregated data.")
                    else:
                        st.info("No data for 6-month trend.")
                with t3:
                    st.subheader("Quarterly Summary")
                    df_s = schm_data()
                    if not df_s.empty and "TXN_DT" in df_s.columns:
                        df_s["TXN_DT"] = pd.to_datetime(df_s["TXN_DT"])
                        fy_now  = today.year if today.month >= 4 else today.year - 1
                        fy_prev = fy_now - 1

                        def map_quarter(dt):
                            y, m = dt.year, dt.month
                            fy = y if m >= 4 else y - 1
                            if m in [4, 5, 6]:     q = "Q1"
                            elif m in [7, 8, 9]:   q = "Q2"
                            elif m in [10, 11, 12]: q = "Q3"
                            else:                   q = "Q4"
                            return f"{q}_FY{fy}"

                        df_s["FY_Q"] = df_s["TXN_DT"].apply(map_quarter)
                        wanted = [f"Q2_FY{fy_prev}", f"Q3_FY{fy_prev}", f"Q4_FY{fy_prev}",
                                  f"Q1_FY{fy_now}",  f"Q2_FY{fy_now}",  f"Q3_FY{fy_now}"]
                        df_s = df_s[df_s["FY_Q"].isin(wanted)]
                        if not df_s.empty:
                            qdf_quarter = df_s.groupby("FY_Q", as_index=False)["AccountsCreated"].sum()
                            qdf_quarter["FY_Q"] = pd.Categorical(qdf_quarter["FY_Q"], wanted)
                            qdf_quarter = qdf_quarter.sort_values("FY_Q")
                            qdf_quarter["Label"] = qdf_quarter["FY_Q"].apply(lambda x: x.replace("_", " (") + ") ")
                            fig_q = px.bar(qdf_quarter, x="Label", y="AccountsCreated",
                                           text="AccountsCreated", color="Label",
                                           color_discrete_sequence=BLUE_PAL)
                            fig_q.update_traces(texttemplate="%{text:,}", textposition="inside",
                                                insidetextanchor="middle", textfont_color="white")
                            st.plotly_chart(apply_theme_to_fig(fig_q, height=320,
                                            x_title="Quarter", y_title="Accounts Created"),
                                            use_container_width=True, key=f"quarter_schm_{iteration}")
                        else:
                            st.info("No quarterly SCHMCODE data available.")
                    else:
                        st.info("No SCHMCODE data available.")

                st.markdown("## Customer Demographics Insights")
                st.markdown(f"""
                <style>
                    .dem-card h3 {{
                        margin: 0 0 10px 0;
                        font-size: 18px;
                        color: {T['TEXT']} !important;
                        font-weight: 700;
                    }}
                </style>""", unsafe_allow_html=True)

                df_demo = demographics_data()
                if not df_demo.empty:
                    df_demo["EMPLOYMENTSTATUS"] = df_demo["EMPLOYMENTSTATUS"].replace(
                        {"Other": "Others", "Un Employed": "Unemployed",
                         "Un employed": "Unemployed", "UN EMPLOYED": "Unemployed"})

                c1, c2, c3 = st.columns(3, gap="large")
                with c1:
                    st.markdown("<div class='dem-card'><h3>Income Source Distribution</h3>", unsafe_allow_html=True)
                    if not df_demo.empty:
                        income_df = df_demo.groupby("INCOMESOURCE", as_index=False)["TOTAL_CUSTOMERS"].sum()
                        fig = px.bar(income_df, x="INCOMESOURCE", y="TOTAL_CUSTOMERS",
                                     text="TOTAL_CUSTOMERS", color_discrete_sequence=BLUE_PAL)
                        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
                        st.plotly_chart(apply_theme_to_fig(fig, height=400),
                                        use_container_width=True, key=f"demo_income_{iteration}")
                    else:
                        st.info("No income source data available.")
                    st.markdown("</div>", unsafe_allow_html=True)
                with c2:
                    st.markdown("<div class='dem-card'><h3>Employment Status</h3>", unsafe_allow_html=True)
                    if not df_demo.empty:
                        emp_df = df_demo.groupby("EMPLOYMENTSTATUS", as_index=False)["TOTAL_CUSTOMERS"].sum()
                        fig = px.pie(emp_df, names="EMPLOYMENTSTATUS", values="TOTAL_CUSTOMERS",
                                     color_discrete_sequence=employment_palette)
                        fig.update_traces(textinfo="label+percent")
                        st.plotly_chart(apply_theme_to_fig(fig, height=400),
                                        use_container_width=True, key=f"demo_employment_{iteration}")
                    else:
                        st.info("No employment status data available.")
                    st.markdown("</div>", unsafe_allow_html=True)
                with c3:
                    st.markdown("<div class='dem-card'><h3>Educational Qualification</h3>", unsafe_allow_html=True)
                    if not df_demo.empty:
                        edu_df = df_demo.groupby("EDUQUALIFICATION", as_index=False)["TOTAL_CUSTOMERS"].sum()
                        fig = px.bar(edu_df, x="EDUQUALIFICATION", y="TOTAL_CUSTOMERS",
                                     text="TOTAL_CUSTOMERS", color_discrete_sequence=BLUE_PAL)
                        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
                        st.plotly_chart(apply_theme_to_fig(fig, height=400),
                                        use_container_width=True, key=f"demo_education_{iteration}")
                    else:
                        st.info("No education qualification data available.")
                    st.markdown("</div>", unsafe_allow_html=True)

                st.markdown("---")
                st.caption(f"Rendered: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | v11.0-secure")

            time.sleep(max(1, int(AUTO_REFRESH_SEC)))

    except Exception:
        logging.exception("Dashboard loop error")
        st.error("An unexpected error occurred.")

    if not st.session_state.get("authenticated", False):
        st.rerun()
