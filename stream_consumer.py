"""
stream_consumer.py — Real-time Micro-batch Consumer & Data Operations Monitor
Continuously processes incoming streaming records from dbo.stg_sales_stream into EcommerceDW.

Features:
- Calls dbo.sp_Consume_MicroBatch with configurable batch size and polling interval.
- Computes and logs processing throughput (records/sec) and execution latency (ms).
- Buffer lag monitoring: Alerts when unprocessed backlog exceeds alert threshold.
- Graceful shutdown handling (SIGINT / Ctrl+C) and database reconnection on failure.
"""

import argparse
import sys
import time
import logging
from datetime import datetime

try:
    import pyodbc
except ImportError:
    print("[ERROR] pyodbc is required. Install via: pip install pyodbc")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("stream_consumer.log", encoding="utf-8")
    ]
)
logger = logging.getLogger("DataOpsConsumer")

# ---------------------------------------------------------------------------
# Default Settings
# ---------------------------------------------------------------------------
DEFAULT_SERVER = r".\SQLEXPRESS"
DEFAULT_DB = "EcommerceDW"
DEFAULT_BATCH_SIZE = 200
DEFAULT_POLL_INTERVAL = 3.0   # seconds
LAG_ALERT_THRESHOLD = 500      # alert if unprocessed rows exceed this

def get_connection(server: str, db: str):
    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={db};Trusted_Connection=yes;"
    return pyodbc.connect(conn_str, autocommit=True)

def check_buffer_depth(cursor) -> int:
    cursor.execute("SELECT COUNT(*) FROM dbo.stg_sales_stream WHERE is_processed = 0;")
    row = cursor.fetchone()
    return row[0] if row else 0

def run_consumer(server: str, db: str, batch_size: int, poll_interval: float):
    logger.info("================================================================")
    logger.info("Starting EcommerceDW Stream Consumer (Data Operations Mode)")
    logger.info(f"Target Database : {server} / {db}")
    logger.info(f"Batch Size      : {batch_size} rows/run")
    logger.info(f"Poll Interval   : {poll_interval}s")
    logger.info(f"Lag Threshold   : {LAG_ALERT_THRESHOLD} rows")
    logger.info("Press Ctrl+C to stop gracefully.")
    logger.info("================================================================")

    total_processed = 0
    cycle = 0

    while True:
        try:
            conn = get_connection(server, db)
            cursor = conn.cursor()
            logger.info("Database connection established.")

            while True:
                cycle += 1
                unprocessed = check_buffer_depth(cursor)

                if unprocessed >= LAG_ALERT_THRESHOLD:
                    logger.warning(f"[LAG ALERT] Buffer backlog high! Pending rows: {unprocessed}")

                if unprocessed > 0:
                    start_time = time.perf_counter()

                    # Execute micro-batch ingestion
                    cursor.execute(f"EXEC dbo.sp_Consume_MicroBatch @batch_size = {batch_size};")
                    elapsed_ms = (time.perf_counter() - start_time) * 1000.0

                    processed_this_run = min(unprocessed, batch_size)
                    total_processed += processed_this_run
                    throughput = (processed_this_run / (elapsed_ms / 1000.0)) if elapsed_ms > 0 else 0

                    logger.info(
                        f"[Cycle {cycle:04d}] Processed {processed_this_run} rows | "
                        f"Latency: {elapsed_ms:.1f}ms | Throughput: {throughput:.0f} r/s | "
                        f"Remaining backlog: {unprocessed - processed_this_run}"
                    )
                else:
                    if cycle % 10 == 0:
                        logger.info(f"[Idle] Buffer empty. Total lifetime consumed: {total_processed} rows.")

                time.sleep(poll_interval)

        except pyodbc.Error as odbc_err:
            logger.error(f"Database error: {odbc_err}. Retrying in 5 seconds...")
            time.sleep(5.0)
        except KeyboardInterrupt:
            logger.info("Shutdown signal received. Exiting consumer cleanly...")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}. Retrying in 5 seconds...")
            time.sleep(5.0)

def main():
    parser = argparse.ArgumentParser(description="EcommerceDW Real-Time Micro-batch Consumer")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="SQL Server instance (default: .\\SQLEXPRESS)")
    parser.add_argument("--db", default=DEFAULT_DB, help="Database name (default: EcommerceDW)")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Rows per micro-batch")
    parser.add_argument("--interval", type=float, default=DEFAULT_POLL_INTERVAL, help="Poll interval in seconds")
    args = parser.parse_args()

    run_consumer(args.server, args.db, args.batch_size, args.interval)

if __name__ == "__main__":
    main()
