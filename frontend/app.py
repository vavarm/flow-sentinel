import streamlit as st
from streamlit_calendar import calendar
from streamlit_js_eval import streamlit_js_eval
import plotly.express as px
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

def generate_pdf(df_prod, date_str, tz_name, total_val, fig):
    """Aggregates and generates PDF report bytes."""
    df_hourly = df_prod.sort("Hour")

    img_bytes = fig.to_image(format="png", width=800, height=450, scale=2)
    
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("helvetica", 'B', 16)
    pdf.cell(0, 10, f"Production Summary - {date_str}", align='C', new_x="LMARGIN", new_y="NEXT")
    
    pdf.set_font("helvetica", size=12)
    pdf.ln(10)
    pdf.cell(0, 10, f"Timezone: {tz_name}", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("helvetica", 'B', 12)
    pdf.cell(0, 10, f"Total Production: {total_val:,.2f}", new_x="LMARGIN", new_y="NEXT")
    
    pdf.image(img_bytes, x=10, y=pdf.get_y() + 5, w=190)
    pdf.set_y(pdf.get_y() + 110)
    
    pdf.ln(5)
    # Table Header
    pdf.set_font("helvetica", 'B', 10)
    pdf.cell(90, 10, "Hour (Local)", 1)
    pdf.cell(90, 10, "Production", 1, new_x="LMARGIN", new_y="NEXT")
    
    # Table Rows
    pdf.set_font("helvetica", size=10)
    for row in df_hourly.select(["Hour", "hourly_val"]).iter_rows():
        hour_label = row[0].strftime('%H:00')
        val_label = f"{row[1]:,.2f}"
        
        pdf.cell(90, 10, hour_label, 1)
        pdf.cell(90, 10, val_label, 1, new_x="LMARGIN", new_y="NEXT")
    
    return bytes(pdf.output())

@st.cache_data(show_spinner=False)
def get_pdf_data(df_prod, date_str, user_tz_name, total_prod, _fig):
    return generate_pdf(df_prod, date_str, user_tz_name, total_prod, _fig)

@st.fragment
def pdf_download_section(df_prod, date_str, user_tz_name, total_prod, fig):
    with st.spinner("🖋️ Preparing PDF Report..."):
        pdf_bytes = get_pdf_data(df_prod, date_str, user_tz_name, total_prod, fig)
        
    st.download_button(
        label="📄 Download PDF Report",
        data=pdf_bytes,
        file_name=f"production_{date_str}.pdf",
        mime="application/pdf",
        use_container_width=True
    )

# --- SIDEBAR ---

st.sidebar.title("Configuration")

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
    SELECT timestamp_floor('1h', ts) as ts, sum(val) as hourly_val
    FROM metrics 
    WHERE ts BETWEEN '{start_utc.strftime('%Y-%m-%dT%H:%M:%S')}' 
    AND '{end_utc.strftime('%Y-%m-%dT%H:%M:%S')}'
    SAMPLE BY 1h;
    """
    
    df_prod = fetch_data(query)

    if not df_prod.is_empty():
        df_prod = df_prod.with_columns(
            pl.col("ts").dt.replace_time_zone("UTC").dt.convert_time_zone(user_tz_name).alias("Hour")
        )
        
        total_prod = df_prod["hourly_val"].sum()
        
        # Dashboard KPIs
        plot_start = datetime.combine(date_obj, datetime.min.time())
        plot_end = datetime.combine(date_obj, datetime.max.time())

        fig = px.bar(
            df_prod.to_pandas(), 
            x="Hour", 
            y="hourly_val",
            labels={"Hour": "Time of Day", "hourly_val": "Production"},
            template="plotly_white"
        )

        fig.update_layout(
            xaxis_range=[plot_start, plot_end],
            xaxis_dtick=3600000 * 1,
            xaxis_tickformat="%H:%M",
            height=450,
            margin=dict(l=20, r=20, t=20, b=20),
        )

        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

        # One-click Download
        pdf_download_section(df_prod, date_str, user_tz_name, total_prod, fig)
    else:
        # Zero-state fallback
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Production", "0.00")
        c2.metric("Average", "0.00")
        c3.metric("Peak", "0.00")
        st.info(f"No production data recorded for {date_str}.")