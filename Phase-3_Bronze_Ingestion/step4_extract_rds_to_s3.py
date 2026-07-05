"""
=============================================================================
PHASE 3 — STEP 4: EXTRACT RDS SQL SERVER TABLES → S3 BRONZE ZONE
=============================================================================
Purpose  : Read all 4 banking tables from RDS SQL Server and write
           them as Parquet files to S3 bronze/ zone with metadata columns
Run      : python step4_extract_rds_to_s3.py
Expected : 4 Parquet files in S3 bronze/ zone
           1 run log entry in metadata/run_logs/

WHAT THIS SCRIPT DOES FOR EACH TABLE:
  branches    → FULL LOAD   : reads all rows every time (small table, 5 rows)
  customers   → FULL LOAD   : reads all rows (first run = historical load)
  accounts    → FULL LOAD   : reads all rows (first run = historical load)
  transactions→ FULL LOAD   : reads all rows (first run = historical load)

  NOTE: This is the FIRST RUN (historical load).
        After Phase 4 (ingestion pipeline), loads become INCREMENTAL
        using the watermark timestamps stored in metadata/watermarks/watermark.json

BRONZE PARQUET OUTPUT (partitioned by load_date):
  s3://ameriprise-bank-datalake/bronze/branches/branches_2026-04-19.parquet
  s3://ameriprise-bank-datalake/bronze/customers/customers_2026-04-19.parquet
  s3://ameriprise-bank-datalake/bronze/accounts/accounts_2026-04-19.parquet
  s3://ameriprise-bank-datalake/bronze/transactions/transactions_2026-04-19.parquet
=============================================================================
"""

import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import hashlib
import uuid
import json
import io
from datetime import datetime, timezone
from botocore.exceptions import ClientError


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — UPDATE RDS_ENDPOINT
# ─────────────────────────────────────────────────────────────
RDS_ENDPOINT = "ap-bank-sqlserver.cxis6seisyzq.ap-south-1.rds.amazonaws.com"  # ← CHANGE THIS
RDS_PORT     = 1433
RDS_USER     = "admin"
RDS_PASSWORD = "BankAdmin#2025"
RDS_DATABASE = "ap_bank_db"
DRIVER       = "ODBC Driver 18 for SQL Server"

AWS_REGION   = "ap-south-1"
BUCKET_NAME  = "neo-bank-datalake"

# Run metadata
BATCH_ID = str(uuid.uuid4())
RUN_TS   = datetime.now(timezone.utc).isoformat()
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# ─────────────────────────────────────────────────────────────
# TABLE EXTRACTION CONFIGS
# ─────────────────────────────────────────────────────────────
TABLE_CONFIGS = [
    {
        "table":        "banking.branches",
        "name":         "branches",
        "load_type":    "FULL_LOAD",
        "s3_prefix":    "bronze/branches",
        "pk":           "branch_code",
        "watermark_col": None,
        "sql":          "SELECT branch_code, branch_name, city, state, region, created_at FROM banking.branches",
        "expected_min": 5,
        "description":  "Bank branches — 5 branches BR001-BR005",
    },
    {
        "table":        "banking.customers",
        "name":         "customers",
        "load_type":    "FULL_LOAD",
        "s3_prefix":    "bronze/customers",
        "pk":           "customer_id",
        "watermark_col": "updated_at",
        "sql":          """
            SELECT customer_id, first_name, last_name, date_of_birth,
                   pan_number, email, phone_number, kyc_status,
                   branch_code, created_at, updated_at
            FROM banking.customers
            ORDER BY customer_id
        """,
        "expected_min": 100,
        "description":  "Bank customers with KYC data",
    },
    {
        "table":        "banking.accounts",
        "name":         "accounts",
        "load_type":    "FULL_LOAD",
        "s3_prefix":    "bronze/accounts",
        "pk":           "account_id",
        "watermark_col": "updated_at",
        "sql":          """
            SELECT account_id, customer_id, account_type, balance,
                   currency, branch_code, status, opened_date,
                   created_at, updated_at
            FROM banking.accounts
            ORDER BY account_id
        """,
        "expected_min": 100,
        "description":  "Bank accounts — Savings and Current",
    },
    {
        "table":        "banking.transactions",
        "name":         "transactions",
        "load_type":    "FULL_LOAD",
        "s3_prefix":    "bronze/transactions",
        "pk":           "txn_id",
        "watermark_col": "txn_timestamp",
        "sql":          """
            SELECT txn_id, account_id, txn_type, amount,
                   txn_timestamp, channel, status, created_at
            FROM banking.transactions
            ORDER BY txn_id
        """,
        "expected_min": 1000,
        "description":  "All banking transactions — UPI/ATM/NEFT/IMPS",
    },
]


# ─────────────────────────────────────────────────────────────
# HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────

def get_rds_connection():
    """Create and return a pyodbc connection to RDS SQL Server."""
    conn_str = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={RDS_ENDPOINT},{RDS_PORT};"
        f"DATABASE={RDS_DATABASE};"
        f"UID={RDS_USER};"
        f"PWD={RDS_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str, timeout=30)


def compute_row_hash(row: pd.Series) -> str:
    """MD5 hash of all source column values — detects row changes."""
    concat = "|".join(str(v) for v in row.values)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def add_metadata_columns(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """Add 8 pipeline metadata columns to every row."""
    df = df.copy()

    # Compute hash BEFORE adding metadata columns
    source_cols    = list(df.columns)
    df["row_hash"] = df[source_cols].apply(compute_row_hash, axis=1)

    df["src_system"]     = "banking_rds"
    df["src_table"]      = config["table"]
    df["src_file_name"]  = f"{config['name']}_{RUN_DATE}.parquet"
    df["load_timestamp"] = RUN_TS
    df["load_date"]      = RUN_DATE
    df["batch_id"]       = BATCH_ID
    df["is_active"]      = 1
    df["pipeline_phase"] = "bronze"
    df["load_type"]      = config["load_type"]

    return df


def convert_datetime_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert datetime columns to strings for Parquet compatibility.
    pyodbc returns datetime objects; Parquet handles them but
    converting to ISO string makes them more portable.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            # Try to detect datetime-like objects
            sample = df[col].dropna().head(1)
            if len(sample) > 0 and hasattr(sample.iloc[0], 'isoformat'):
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if x is not None else None
                )
    return df


def df_to_s3_parquet(df: pd.DataFrame, config: dict, s3_client) -> str:
    """
    Convert DataFrame to Parquet and upload directly to S3.
    Uses in-memory buffer (no temp files on disk).
    Returns the S3 key of the uploaded file.
    """
    # Build PyArrow table with schema metadata
    table = pa.Table.from_pandas(df, preserve_index=False)

    custom_meta = {
        "project":      "ameriprise-bank-de-pipeline",
        "source_table": config["table"],
        "load_type":    config["load_type"],
        "batch_id":     BATCH_ID,
        "run_ts":       RUN_TS,
        "row_count":    str(len(df)),
        "phase":        "phase3-rds-extraction",
    }
    merged_meta = {
        **table.schema.metadata,
        b"custom_metadata": json.dumps(custom_meta).encode()
    }
    table = table.replace_schema_metadata(merged_meta)

    # Write to in-memory buffer
    buf = io.BytesIO()
    pq.write_table(
        table, buf,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
    )
    buf.seek(0)

    # S3 key: bronze/customers/customers_2026-04-19.parquet
    s3_key = f"{config['s3_prefix']}/{config['name']}_{RUN_DATE}.parquet"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
        Metadata={
            "project":      "ameriprise-bank-de-pipeline",
            "data-zone":    "bronze",
            "source-table": config["table"],
            "row-count":    str(len(df)),
            "load-type":    config["load_type"],
            "batch-id":     BATCH_ID[:8],
        }
    )
    return s3_key


def update_watermark(s3_client, source_name: str, run_ts: str,
                     rows_loaded: int, status: str, last_watermark: str = None):
    """
    Update the watermark.json file in S3 after successful load.
    Phase 4 reads this to know 'what was the last run timestamp'.
    """
    watermark_key = "metadata/watermarks/watermark.json"

    # Read existing watermark
    try:
        resp          = s3_client.get_object(Bucket=BUCKET_NAME, Key=watermark_key)
        watermark_doc = json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError:
        watermark_doc = {"_meta": {}, "sources": {}}

    # Update this source's watermark
    watermark_doc["sources"][source_name] = {
        "last_successful_run": run_ts,
        "last_run_status":     status,
        "load_type":           "FULL_LOAD",
        "watermark_column":    None,
        "rows_last_loaded":    rows_loaded,
        "last_batch_id":       BATCH_ID,
        "last_updated":        RUN_TS,
    }
    watermark_doc["_meta"]["last_updated"] = RUN_TS

    # Write back to S3
    body = json.dumps(watermark_doc, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=watermark_key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def save_run_log(s3_client, results: list):
    """Save a run log JSON to metadata/run_logs/ for audit trail."""
    log = {
        "batch_id":    BATCH_ID,
        "run_ts":      RUN_TS,
        "run_date":    RUN_DATE,
        "phase":       "phase3-rds-extraction",
        "pipeline":    "banking_historical_load",
        "status":      "SUCCESS" if all(r["status"] == "SUCCESS" for r in results) else "PARTIAL",
        "tables":      results,
        "total_rows":  sum(r.get("rows_loaded", 0) for r in results),
    }
    key  = f"metadata/run_logs/phase3_run_{RUN_DATE}_{BATCH_ID[:8]}.json"
    body = json.dumps(log, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    print(f"\n  Run log saved: s3://{BUCKET_NAME}/{key}")
    return key


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 3 Step 4: RDS → S3 Bronze Extract")
    print("=" * 65)
    print(f"\n  Batch ID  : {BATCH_ID}")
    print(f"  Run Date  : {RUN_DATE}")
    print(f"  Run Time  : {RUN_TS}")
    print(f"  Source    : {RDS_ENDPOINT}")
    print(f"  Target    : s3://{BUCKET_NAME}/bronze/")

    if "YOUR-RDS-ENDPOINT" in RDS_ENDPOINT:
        print("\n  [ERROR] Update RDS_ENDPOINT at the top of this script!")
        return

    # Establish connections
    print(f"\n  Connecting to RDS SQL Server...")
    try:
        conn   = get_rds_connection()
        s3     = boto3.client("s3", region_name=AWS_REGION)
        print(f"  [OK]  RDS connected")
        print(f"  [OK]  S3 client ready")
    except Exception as e:
        print(f"  [FAIL] Connection error: {e}")
        return

    results = []

    # ── Extract each table ─────────────────────────────────────
    for config in TABLE_CONFIGS:
        print(f"\n{'─'*65}")
        print(f"  Table     : {config['table']}")
        print(f"  Load type : {config['load_type']}")
        print(f"  Query     : {config['sql'].strip()[:80]}...")

        try:
            # Step A: Extract from RDS
            print(f"\n  [A] Extracting from RDS...")
            df = pd.read_sql(config["sql"], conn)
            print(f"      Rows extracted : {len(df):,}")
            print(f"      Columns        : {list(df.columns)}")

            if len(df) < config["expected_min"]:
                print(f"      [WARN] Expected at least {config['expected_min']:,} rows")
                print(f"             Did you run all 3 SQL scripts in DBeaver?")

            # Step B: Convert datetime objects to strings
            print(f"\n  [B] Converting data types...")
            df = convert_datetime_cols(df)

            # Step C: Add metadata columns
            print(f"\n  [C] Adding metadata columns...")
            df = add_metadata_columns(df, config)
            print(f"      Final shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

            # Step D: Upload to S3
            print(f"\n  [D] Uploading to S3 bronze zone...")
            s3_key = df_to_s3_parquet(df, config, s3)
            print(f"      [OK]  s3://{BUCKET_NAME}/{s3_key}")

            # Step E: Verify in S3
            print(f"\n  [E] Verifying upload...")
            resp     = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            size_kb  = resp["ContentLength"] / 1024
            print(f"      Size in S3: {size_kb:.1f} KB  ✓")

            # Step F: Update watermark
            update_watermark(s3, f"banking.{config['name']}", RUN_TS,
                             len(df), "SUCCESS")
            print(f"      Watermark updated for banking.{config['name']}")

            results.append({
                "table":       config["table"],
                "s3_key":      s3_key,
                "rows_loaded": len(df),
                "status":      "SUCCESS",
                "size_kb":     round(size_kb, 1),
            })

        except Exception as e:
            print(f"\n  [FAIL] {config['table']}: {e}")
            results.append({
                "table":  config["table"],
                "status": "FAILED",
                "error":  str(e),
            })

    conn.close()

    # ── Save run log ────────────────────────────────────────────
    save_run_log(s3, results)

    # ── Summary ─────────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  EXTRACTION SUMMARY")
    print(f"{'='*65}")
    print(f"  {'Table':<30} {'Status':<10} {'Rows':>10} {'Size':>8}")
    print(f"  {'-'*30} {'-'*10} {'-'*10} {'-'*8}")
    for r in results:
        rows = f"{r.get('rows_loaded',0):,}" if r["status"]=="SUCCESS" else "-"
        size = f"{r.get('size_kb',0):.1f}KB"  if r["status"]=="SUCCESS" else "-"
        print(f"  {r['table']:<30} {r['status']:<10} {rows:>10} {size:>8}")

    ok_count = sum(1 for r in results if r["status"] == "SUCCESS")
    print(f"\n  {ok_count}/{len(results)} tables extracted successfully")
    if ok_count == len(results):
        print(f"\n  NEXT STEP: Run step5_verify_bronze_layer.py")
    print("=" * 65)


if __name__ == "__main__":
    main()
