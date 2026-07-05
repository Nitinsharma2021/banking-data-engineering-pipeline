"""
=============================================================================
PHASE 5 — STEP 1: CREATE DIM_DATE TABLE → UPLOAD TO S3 GOLD ZONE
=============================================================================
Purpose  : Generate a complete date dimension table (2020-01-01 to 2030-12-31)
           and upload it to s3://ameriprise-bank-datalake/gold/dim_date/
Run      : python step1_create_dim_date.py
Expected : dim_date Parquet file in S3 with 3,653 rows

WHY DIM_DATE:
  Every fact table (fact_transactions, fact_payments, fact_credit_risk)
  has a date column. dim_date gives you year/month/quarter/week/is_weekend
  so BI tools can group/filter by ANY time period without SQL date functions.

  Example query that becomes possible:
    SELECT d.quarter, SUM(f.amount)
    FROM fact_transactions f
    JOIN dim_date d ON f.txn_date = d.full_date
    WHERE d.year = 2025
    GROUP BY d.quarter

DIM_DATE COLUMNS:
  date_sk        → surrogate key (int, e.g. 20250115 = YYYYMMDD format)
  full_date      → "2025-01-15" (joins to fact table date columns)
  day_of_month   → 15
  day_of_week    → 3 (1=Mon, 7=Sun)
  day_name       → "Wednesday"
  week_of_year   → 3
  month_num      → 1
  month_name     → "January"
  quarter        → 1
  year           → 2025
  is_weekend     → 0 (1 for Sat/Sun)
  is_month_end   → 0 (1 for last day of month)
  is_month_start → 1 (1 for first day of month)
  fiscal_year    → 2026 (April start — Indian fiscal year)
  fiscal_quarter → Q4   (Indian fiscal year: Apr-Jun=Q1, Jan-Mar=Q4)
=============================================================================
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import io
from datetime import datetime, timezone, date, timedelta


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION   = "ap-south-1"
BUCKET_NAME  = "ameriprise-bank-datalake"
S3_KEY       = "gold/dim_date/dim_date.parquet"
START_DATE   = date(2020, 1, 1)
END_DATE     = date(2030, 12, 31)


def get_fiscal_year(d: date) -> int:
    """Indian fiscal year: Apr to Mar. Jan 2025 = FY2025 (Apr2024-Mar2025)."""
    return d.year if d.month >= 4 else d.year


def get_fiscal_quarter(d: date) -> str:
    """Indian fiscal quarters: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar."""
    m = d.month
    if   m in [4, 5, 6]:  return "Q1"
    elif m in [7, 8, 9]:  return "Q2"
    elif m in [10,11,12]: return "Q3"
    else:                  return "Q4"   # Jan, Feb, Mar


def build_dim_date(start: date, end: date) -> pd.DataFrame:
    """Generate all date rows from start to end (inclusive)."""
    rows = []
    current = start
    day_names   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    month_names = ["January","February","March","April","May","June",
                   "July","August","September","October","November","December"]

    while current <= end:
        # date_sk: integer YYYYMMDD e.g. 20250115
        date_sk    = int(current.strftime("%Y%m%d"))
        full_date  = current.strftime("%Y-%m-%d")

        # Is last day of month?
        if current.month == 12:
            next_month_first = date(current.year + 1, 1, 1)
        else:
            next_month_first = date(current.year, current.month + 1, 1)
        is_month_end   = 1 if (current + timedelta(days=1)) == next_month_first else 0
        is_month_start = 1 if current.day == 1 else 0

        rows.append({
            "date_sk":        date_sk,
            "full_date":      full_date,
            "day_of_month":   current.day,
            "day_of_week":    current.isoweekday(),       # 1=Mon, 7=Sun
            "day_name":       day_names[current.weekday()],
            "week_of_year":   current.isocalendar()[1],
            "month_num":      current.month,
            "month_name":     month_names[current.month - 1],
            "quarter":        (current.month - 1) // 3 + 1,  # calendar quarter
            "year":           current.year,
            "is_weekend":     1 if current.weekday() >= 5 else 0,  # Sat=5, Sun=6
            "is_month_end":   is_month_end,
            "is_month_start": is_month_start,
            "fiscal_year":    get_fiscal_year(current),
            "fiscal_quarter": get_fiscal_quarter(current),
        })
        current += timedelta(days=1)

    return pd.DataFrame(rows)


def upload_to_s3(df: pd.DataFrame, bucket: str, key: str, s3_client):
    """Convert to Parquet and upload to S3."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="snappy", write_statistics=True)
    buf.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
        Metadata={
            "table-name":  "dim_date",
            "data-zone":   "gold",
            "row-count":   str(len(df)),
            "date-range":  f"{START_DATE} to {END_DATE}",
            "project":     "ameriprise-bank-de-pipeline",
        }
    )


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 5 Step 1: Create dim_date")
    print("=" * 65)

    # ── Build dim_date ──────────────────────────────────────
    print(f"\n[1] Generating date dimension ({START_DATE} to {END_DATE})...")
    df = build_dim_date(START_DATE, END_DATE)
    print(f"    Rows generated : {len(df):,}")
    print(f"    Columns        : {list(df.columns)}")

    print(f"\n[2] Sample rows:")
    sample = df[df["full_date"].isin(["2025-01-01","2025-03-31","2025-04-01","2025-12-31"])]
    print(sample[["date_sk","full_date","day_name","month_name","quarter",
                   "is_weekend","fiscal_year","fiscal_quarter"]].to_string(index=False))

    print(f"\n[3] Uploading to S3...")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    upload_to_s3(df, BUCKET_NAME, S3_KEY, s3)

    # ── Verify ──────────────────────────────────────────────
    print(f"\n[4] Verifying in S3...")
    resp     = s3.head_object(Bucket=BUCKET_NAME, Key=S3_KEY)
    size_kb  = resp["ContentLength"] / 1024
    print(f"    [OK]  s3://{BUCKET_NAME}/{S3_KEY}")
    print(f"    Size : {size_kb:.1f} KB")
    print(f"    Rows : {len(df):,} dates ({START_DATE} to {END_DATE})")

    print(f"\n{'='*65}")
    print(f"  dim_date CREATED SUCCESSFULLY")
    print(f"\n  HOW dim_date JOINS TO FACT TABLES:")
    print(f"    fact_transactions.txn_date   = dim_date.full_date")
    print(f"    fact_payments.txn_date       = dim_date.full_date")
    print(f"    fact_credit_risk.bureau_pull_date = dim_date.full_date")
    print(f"\n  NEXT STEP: Create all 7 Glue Visual ETL Gold jobs")
    print(f"             (follow PHASE5_COMPLETE_README.txt)")
    print("=" * 65)


if __name__ == "__main__":
    main()
