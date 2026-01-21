import time
import random
import threading
import logging
import sys
import psycopg2
from flask import Flask
from questdb.ingress import Sender, TimestampNanos
from apscheduler.schedulers.background import BackgroundScheduler

# --- 1. CONFIGURATION & LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

DB_PARAMS = {
    "host": "questdb",
    "port": 8812,
    "user": "admin",
    "password": "quest",
    "database": "qdb",
}


# --- 2. MAINTENANCE (CLEANUP) LOGIC ---
def daily_cleanup():
    """Drops partitions older than 7 days to manage disk space."""
    logger.info(
        "MAINTENANCE: Starting scheduled partition cleanup (Retention: 7 days)."
    )
    try:
        with psycopg2.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                # Optimized Drop for QuestDB
                cur.execute("ALTER TABLE pulse DROP PARTITION WHERE ts < now() - 7d;")
                cur.execute("ALTER TABLE metrics DROP PARTITION WHERE ts < now() - 7d;")
                cur.execute("ALTER TABLE events DROP PARTITION WHERE ts < now() - 7d;")
        logger.info(
            "MAINTENANCE: Cleanup successful. Partitions older than 7 days removed."
        )
    except Exception as e:
        logger.error(f"MAINTENANCE: Cleanup failed. Reason: {e}")


# --- 3. DATABASE INITIALIZATION ---
def init_db():
    """Ensures tables exist and are optimized for time-series."""
    while True:
        try:
            with psycopg2.connect(**DB_PARAMS) as conn:
                with conn.cursor() as cur:
                    # Create tables with Designated Timestamps
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS metrics (val DOUBLE, ts TIMESTAMP) 
                        TIMESTAMP(ts) PARTITION BY DAY;
                    """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS pulse (status INT, ts TIMESTAMP) 
                        TIMESTAMP(ts) PARTITION BY DAY;
                    """
                    )
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS events (msg SYMBOL, ts TIMESTAMP) 
                        TIMESTAMP(ts) PARTITION BY DAY;
                    """
                    )
                    # Bypass WAL for near-instant visibility in monitoring use-cases
                    cur.execute("ALTER TABLE pulse SET TYPE BYPASS WAL;")
                    cur.execute("ALTER TABLE metrics SET TYPE BYPASS WAL;")
            logger.info(
                "DATABASE: Tables synchronized and optimized (BYPASS WAL enabled)."
            )
            break
        except Exception as e:
            logger.warning(f"DATABASE: Waiting for QuestDB... ({e})")
            time.sleep(2)


# --- 4. BACKGROUND WORKER (DATA INGESTION) ---
def worker():
    """Generates mock pulse and metrics data."""
    logger.info("INGRESS: Background worker started.")
    try:
        with Sender("tcp", "questdb", 9009) as sender:
            while True:
                now = TimestampNanos.now()
                val = random.normalvariate(50, 15)

                sender.row("metrics", columns={"val": val}, at=now)
                sender.row("pulse", columns={"status": 1}, at=now)

                # Ingress is buffered; flush happens automatically or on close
                sender.flush()
                time.sleep(2)
    except Exception as e:
        logger.error(f"INGRESS: Worker crashed. Reason: {e}")


# --- 5. FLASK ROUTES ---
@app.route("/pulse/event/<msg>")
def log_event(msg):
    try:
        with Sender("tcp", "questdb", 9009) as sender:
            sender.row("events", symbols={"msg": msg}, at=TimestampNanos.now())
            sender.flush()
        logger.info(f"EVENT: Captured custom signal: {msg}")
        return {"status": "captured", "message": msg}
    except Exception as e:
        logger.error(f"EVENT: Failed to log event. Error: {e}")
        return {"status": "error", "message": str(e)}, 500


# --- 6. EXECUTION ---
if __name__ == "__main__":
    init_db()

    # Start the Cron Scheduler
    scheduler = BackgroundScheduler()
    # Runs at 00:00 every day
    scheduler.add_job(daily_cleanup, "cron", hour=0, minute=0)
    scheduler.start()
    logger.info("SCHEDULER: Cleanup job scheduled for daily execution at 00:00.")

    # Start the data generation thread
    threading.Thread(target=worker, daemon=True).start()

    # Run the Web Server
    logger.info("APP: Starting FlowSentinel API on port 5000.")
    app.run(host="0.0.0.0", port=5000)
