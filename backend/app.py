import time
import random
import threading
import logging
import sys
import psycopg
import logging_loki
from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from questdb.ingress import Sender, TimestampNanos
from apscheduler.schedulers.background import BackgroundScheduler

# --- 1. CONFIGURATION & LOGGING ---
console_handler = logging.StreamHandler(sys.stdout)
console_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", 
    datefmt="%Y-%m-%d %H:%M:%S"
)
console_handler.setFormatter(console_formatter)

loki_remote_handler = logging_loki.LokiHandler(
    url="http://loki:3100/loki/api/v1/push", 
    tags={"application": "flow-sentinel"},
    version="1",
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(loki_remote_handler)

DB_PARAMS = {
    "host": "questdb",
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "dbname": "qdb",
    "prepare_threshold": None,
}

# --- 2. PERSISTENT CONNECTIONS ---
# Initialize the Sender once at global level using ILP (TCP) on port 9009
quest_sender = Sender("tcp", "questdb", 9009)

# --- 3. MAINTENANCE (CLEANUP) LOGIC ---
def daily_cleanup():
    """Drops partitions older than 7 days to manage disk space."""
    logger.info("MAINTENANCE: Starting scheduled partition cleanup (Retention: 7 days).")
    try:
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE metrics DROP PARTITION WHERE ts < now() - 7d;")
        logger.info("MAINTENANCE: Cleanup successful.")
    except Exception as e:
        logger.error(f"MAINTENANCE: Cleanup failed. Reason: {e}")

# --- 4. DATABASE INITIALIZATION ---
def init_db():
    """Ensures tables exist and are optimized for time-series."""
    while True:
        try:
            with psycopg.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    # Create tables with Designated Timestamps
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS metrics (val DOUBLE, ts TIMESTAMP) 
                        TIMESTAMP(ts) PARTITION BY DAY;
                    """)
                    # Bypass WAL for near-instant visibility
                    cur.execute("ALTER TABLE metrics SET TYPE BYPASS WAL;")
            logger.info("DATABASE: Tables synchronized and optimized (BYPASS WAL enabled).")
            break
        except Exception as e:
            logger.warning(f"DATABASE: Waiting for QuestDB... ({e})")
            time.sleep(2)

# --- 5. BACKGROUND WORKER (DATA INGESTION) ---
def worker():
    """Generates mock pulse and metrics data using the persistent sender."""
    logger.info("INGRESS: Background worker started.")
    try:
        while True:
            now = TimestampNanos.now()
            val = random.normalvariate(50, 15)

            # Reusing the global quest_sender
            quest_sender.row("metrics", columns={"val": val}, at=now)
            quest_sender.row("pulse", columns={"status": 1}, at=now)
            quest_sender.flush()
            time.sleep(2)
    except Exception as e:
        logger.error(f"INGRESS: Worker crashed. Reason: {e}")

# --- 7. EXECUTION (Lifespan Management) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup logic
    init_db()
    
    # Opening the ILP connection context
    with quest_sender:
        scheduler = BackgroundScheduler()
        # Scheduled cleanup at midnight
        scheduler.add_job(daily_cleanup, "cron", hour=0, minute=0)
        scheduler.start()
        logger.info("SCHEDULER: Cleanup job scheduled for daily execution at 00:00.")

        # Launching data generation thread
        threading.Thread(target=worker, daemon=True).start()
        
        logger.info("APP: Starting FlowSentinel API on port 5000.")
        yield
        # Shutdown logic
        scheduler.shutdown()

app = FastAPI(lifespan=lifespan)

# --- 6. FASTAPI ROUTES ---
@app.get("/metrics")
async def metrics():
    """Endpoint for Prometheus/Blackbox Exporter scraping."""
    output = "# HELP app_up Status of the application\n# TYPE app_up gauge\napp_up 1\n"
    return Response(content=output, media_type="text/plain")

@app.get("/event/{msg}")
async def log_event(msg: str):
    """Logs events directly to Loki via the logger handler."""
    try:
        # Logs are automatically routed to Loki by the logging config
        logger.info(f"EVENT: {msg}", extra={"tags": {"event_type": "manual_signal"}})
        return {"status": "captured", "message": msg, "destination": "loki"}
    except Exception as e:
        logger.error(f"EVENT: Failed to log. Error: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="info")