import streamlit as st
from streamlit_calendar import calendar
from streamlit_js_eval import streamlit_js_eval
import psycopg
import polars as pl
from fpdf import FPDF
import pytz
from datetime import datetime
import io

# Database Configuration
DB_PARAMS = {
    "host": "questdb",
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "dbname": "qdb",
    "prepare_threshold": None,
}

st.set_page_config(layout="wide", page_title="Production Analytics")

# UI Customization: Hide Deploy/Menu while keeping Sidebar toggle functional
st.markdown("""
    <style>
        /* Targeted hiding to preserve the sidebar toggle arrow */
        .stAppDeployButton { display: none !important; }
        #MainMenu { visibility: hidden; }
        footer { visibility: hidden; }
        
        /* Highlight for selected calendar day */
        .fc-highlight {
            background: rgba(46, 204, 113, 0.4) !important;
            border: 2px solid #27ae60 !important;
        }
    </style>
""", unsafe_allow_html=True)

# --- UTILITIES ---

def get_db_connection():
    return psycopg.connect(**DB_PARAMS)

def fetch_data(query):
    """Executes query and returns Polars DataFrame."""
    try:
        with get_db_connection() as conn:
            return pl.read_database(query, conn)
    except Exception as e:
        st.error(f"Database Error: {e}")
        return pl.DataFrame()

def generate_pdf(df_prod, date_str, tz_name, total_val):
    """Aggregates and generates PDF report bytes."""
    df_hourly = (
        df_prod.with_columns(pl.col("ts_local").dt.truncate("1h"))
        .group_by("ts_local")
        .agg(pl.col("val").sum())
        .sort("ts_local")
    )
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(0, 10, f"Production Summary - {date_str}", ln=True, align='C')
    
    pdf.set_font("Arial", size=12)
    pdf.ln(10)
    pdf.cell(0, 10, f"Timezone: {tz_name}", ln=True)
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(0, 10, f"Total Production: {total_val:,.2f}", ln=True)
    pdf.ln(5)
    
    pdf.set_font("Arial", 'B', 10)
    pdf.cell(90, 10, "Hour (Local)", 1)
    pdf.cell(90, 10, "Hourly Total", 1)
    pdf.ln()
    
    pdf.set_font("Arial", size=10)
    for row in df_hourly.iter_rows():
        pdf.cell(90, 10, row[0].strftime('%H:00'), 1)
        pdf.cell(90, 10, f"{row[1]:,.2f}", 1)
        pdf.ln()
    
    return pdf.output(dest='S').encode('latin-1')

# --- SIDEBAR ---

st.sidebar.title("Configuration")

# Usage stats warning fix: this usually goes in .streamlit/config.toml, 
# but we focus on the UI functionality here.
detected_tz = streamlit_js_eval(js_expressions='Intl.DateTimeFormat().resolvedOptions().timeZone', key='tz_detector')

user_tz_name = st.sidebar.selectbox(
    "Local Timezone", 
    pytz.all_timezones, 
    index=pytz.all_timezones.index(detected_tz) if detected_tz in pytz.all_timezones else pytz.all_timezones.index('UTC')
)

# --- CALENDAR ---

st.title("🏭 Production Monitoring Dashboard")

# Get dates with data
df_days = fetch_data("SELECT DISTINCT timestamp_floor('d', ts) as day FROM metrics")

# Background events to show data presence without text
calendar_events = [
    {"start": str(d), "allDay": True, "display": "background", "color": "#27ae60"} 
    for d in (df_days["day"] if not df_days.is_empty() else [])
]

cal_options = {
    "initialView": "dayGridMonth",
    "selectable": True,
    "unselectAuto": False,
    "timeZone": user_tz_name,
}

# Added 'key' to ensure state persistence across interactions
state = calendar(events=calendar_events, options=cal_options, key="prod_calendar")

# --- INTERACTION LOGIC ---

if "selected_date" not in st.session_state:
    st.session_state.selected_date = None

if state:
    new_date = None
    
    if state.get("dateClick"):
        new_date = state["dateClick"]["date"].split('T')[0]

    elif state.get("select"):
        new_date = state["select"]["start"].split('T')[0]
    
    if new_date and new_date != st.session_state.selected_date:
        st.session_state.selected_date = new_date
        st.rerun()

if st.session_state.selected_date:
    date_str = st.session_state.selected_date
    
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    st.divider()
    st.subheader(f"Data for: {date_str}")

    # Query setup
    user_tz = pytz.timezone(user_tz_name)
    start_utc = user_tz.localize(datetime.combine(date_obj, datetime.min.time())).astimezone(pytz.UTC)
    end_utc = user_tz.localize(datetime.combine(date_obj, datetime.max.time())).astimezone(pytz.UTC)

    query = f"""
    SELECT ts, val FROM metrics 
    WHERE ts >= '{start_utc.strftime('%Y-%m-%dT%H:%M:%S')}' 
    AND ts <= '{end_utc.strftime('%Y-%m-%dT%H:%M:%S')}'
    """
    df_prod = fetch_data(query)

    if not df_prod.is_empty():
        df_prod = df_prod.with_columns(
            pl.col("ts").dt.replace_time_zone("UTC").dt.convert_time_zone(user_tz_name).alias("ts_local")
        )
        
        total_prod = df_prod["val"].sum()
        
        # Dashboard KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Production", f"{total_prod:,.2f}")
        c2.metric("Average", f"{df_prod['val'].mean():.2f}")
        c3.metric("Peak", f"{df_prod['val'].max():.2f}")

        st.line_chart(df_prod.to_pandas(), x="ts_local", y="val")

        # One-click Download
        st.download_button(
            label="📄 Download PDF Report",
            data=generate_pdf(df_prod, date_str, user_tz_name, total_prod),
            file_name=f"production_{date_str}.pdf",
            mime="application/pdf"
        )
    else:
        # Zero-state fallback
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Production", "0.00")
        c2.metric("Average", "0.00")
        c3.metric("Peak", "0.00")
        st.info(f"No production data recorded for {date_str}.")