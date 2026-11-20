"""
stream_producer.py — Micro-batch streaming simulator for EcommerceDW
Reads CSV → inserts into stg_sales_stream with ingest_time watermark.

Usage:
  python stream_producer.py
  python stream_producer.py --batch-size 50 --delay 1.0 --limit 500
  python stream_producer.py --server MYPC\\SQLEXPRESS
"""

import argparse
import csv
import random
import time
import sys
from datetime import datetime
from pathlib import Path

try:
    import pyodbc
except ImportError:
    print("[ERROR] pyodbc not installed. Run: pip install pyodbc")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_SERVER   = r".\SQLEXPRESS"   # change to "." or "localhost" if needed
DEFAULT_DB       = "EcommerceDW"
DEFAULT_CSV      = Path(__file__).parent / "du_lieu_csv" / "ecommerce_sales_customer_analytics_150k.csv"
DEFAULT_BATCH    = 100               # rows per insert batch
DEFAULT_DELAY    = 0.5               # seconds between batches
DEFAULT_LIMIT    = 0                 # 0 = no limit (send all rows)

INSERT_SQL = """
INSERT INTO dbo.stg_sales_stream (
    order_id, order_date, order_time, order_status, sales_channel,
    customer_id, customer_name, customer_age, gender, customer_segment,
    customer_type, customer_city, customer_state, customer_country,
    region, customer_postal_code, payment_method, payment_status,
    currency, shipping_method, warehouse, delivery_days,
    estimated_delivery_days, delivery_status, return_status,
    return_reason, customer_rating, review_sentiment, customer_review,
    marketing_channel, campaign_name, coupon_code,
    loyalty_points_earned, loyalty_points_redeemed,
    quantity, gross_sales, discount_amount, tax_amount,
    shipping_cost, net_sales, product_cost, profit,
    ingest_time
) VALUES (
    ?,?,?,?,?,  ?,?,?,?,?,
    ?,?,?,?,    ?,?,?,?,
    ?,?,?,?,    ?,?,?,
    ?,?,?,?,    ?,?,?,
    ?,?,        ?,?,?,?,
    ?,?,?,?,    SYSDATETIME()
)
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="EcommerceDW Stream Producer")
    p.add_argument("--server",     default=DEFAULT_SERVER)
    p.add_argument("--db",         default=DEFAULT_DB)
    p.add_argument("--csv",        default=str(DEFAULT_CSV))
    p.add_argument("--batch-size", type=int,   default=DEFAULT_BATCH)
    p.add_argument("--delay",      type=float, default=DEFAULT_DELAY)
    p.add_argument("--limit",      type=int,   default=DEFAULT_LIMIT,
                   help="Max rows to send (0 = all)")
    p.add_argument("--no-shuffle", action="store_true",
                   help="Keep CSV row order (default: random shuffle)")
    return p.parse_args()


def connect(server, db):
    conn_str = (
        f"Driver={{ODBC Driver 17 for SQL Server}};"
        f"Server={server};"
        f"Database={db};"
        "Trusted_Connection=yes;"
        "Connection Timeout=10;"
    )
    try:
        conn = pyodbc.connect(conn_str, autocommit=False)
        print(f"[OK] Connected → {server}/{db}")
        return conn
    except pyodbc.Error as e:
        print(f"[ERROR] Cannot connect: {e}")
        print("  Try: --server .  or  --server localhost\\SQLEXPRESS")
        sys.exit(1)



def row_to_params(r):
    """Map CSV DictReader row → INSERT parameter tuple (42 values, matches INSERT_SQL)."""
    return (
        r.get("order_id")              or None,
        r.get("order_date")            or None,
        r.get("order_time")            or None,
        r.get("order_status")          or None,
        r.get("sales_channel")         or None,
        r.get("customer_id")           or None,
        r.get("customer_name")         or None,
        r.get("customer_age")          or None,
        r.get("gender")                or None,
        r.get("customer_segment")      or None,
        r.get("customer_type")         or None,
        r.get("customer_city")         or None,
        r.get("customer_state")        or None,
        r.get("customer_country")      or None,
        r.get("region")                or None,
        r.get("customer_postal_code")  or None,
        r.get("payment_method")        or None,
        r.get("payment_status")        or None,
        r.get("currency")              or "USD",
        r.get("shipping_method")       or None,
        r.get("warehouse")             or None,
        r.get("delivery_days")         or None,
        r.get("estimated_delivery_days") or None,
        r.get("delivery_status")       or None,
        r.get("return_status")         or None,
        r.get("return_reason")         or None,
        r.get("customer_rating")       or None,
        r.get("review_sentiment")      or None,
        r.get("customer_review")       or None,
        r.get("marketing_channel")     or None,
        r.get("campaign_name")         or None,
        r.get("coupon_code")           or None,
        r.get("loyalty_points_earned") or None,
        r.get("loyalty_points_redeemed") or None,
        r.get("quantity")              or None,
        r.get("gross_sales")           or None,
        r.get("discount_amount")       or None,
        r.get("tax_amount")            or None,
        r.get("shipping_cost")         or None,
        r.get("net_sales")             or None,
        r.get("product_cost")          or None,
        r.get("profit")                or None,
    )


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    args = parse_args()
    csv_path = Path(args.csv)

    if not csv_path.exists():
        print(f"[ERROR] CSV not found: {csv_path}")
        sys.exit(1)

    print(f"[INFO] Reading CSV: {csv_path.name}")
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    print(f"[INFO] Loaded {len(rows):,} rows from CSV")

    if not args.no_shuffle:
        random.shuffle(rows)
        print("[INFO] Rows shuffled (simulate random event arrival)")

    if args.limit > 0:
        rows = rows[: args.limit]
        print(f"[INFO] Limited to {args.limit:,} rows")

    conn   = connect(args.server, args.db)
    cursor = conn.cursor()

    total_sent   = 0
    batch_num    = 0
    t_start      = time.time()

    print(f"\n{'─'*55}")
    print(f"  Batch size : {args.batch_size}  |  Delay : {args.delay}s")
    print(f"  Total rows : {len(rows):,}")
    print(f"{'─'*55}")

    for i in range(0, len(rows), args.batch_size):
        batch      = rows[i : i + args.batch_size]
        batch_num += 1
        t0         = time.time()

        try:
            params_list = [row_to_params(r) for r in batch]
            cursor.executemany(INSERT_SQL, params_list)
            conn.commit()
        except pyodbc.Error as e:
            conn.rollback()
            print(f"\n[WARN] Batch {batch_num} failed: {e}")
            continue

        total_sent += len(batch)
        elapsed     = time.time() - t_start
        rps         = total_sent / elapsed if elapsed > 0 else 0
        batch_ms    = (time.time() - t0) * 1000

        print(
            f"  Batch {batch_num:>4}  |  sent {len(batch):>4} rows  "
            f"|  total {total_sent:>7,}  |  {rps:>7.1f} rows/s  "
            f"|  {batch_ms:>6.1f}ms  |  {datetime.now().strftime('%H:%M:%S')}"
        )

        if i + args.batch_size < len(rows):
            time.sleep(args.delay)

    elapsed = time.time() - t_start
    print(f"\n{'─'*55}")
    print(f"[DONE] Sent {total_sent:,} rows in {elapsed:.1f}s "
          f"({total_sent/elapsed:.1f} rows/s avg) | Batches: {batch_num} ")
    print(f"{'─'*55}")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
