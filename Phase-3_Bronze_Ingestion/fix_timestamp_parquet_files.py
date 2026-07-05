"""
=============================================================================
FIX SCRIPT: Regenerate RDS Parquet files with MICROSECOND timestamps
=============================================================================
ROOT CAUSE:
  Your Phase 3 script extracted RDS data and saved Parquet files where
  datetime columns were stored as TIMESTAMP(NANOS) — nanosecond precision.
  AWS Glue Spark only supports TIMESTAMP(MICROS) — microsecond precision.
  Result: Glue crashes with "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))"

AFFECTED FILES (all 4 RDS-extracted bronze Parquet files):
  bronze/branches/branches_2026-04-26.parquet      (created_at)
  bronze/customers/customers_2026-04-26.parquet    (created_at, updated_at)
  bronze/accounts/accounts_2026-04-26.parquet      (created_at, updated_at)
  bronze/transactions/transactions_2026-04-26.parquet (txn_timestamp, created_at)

NOT AFFECTED (CSV-sourced files — timestamps stored as strings):
  bronze/payment_gateway_logs/...parquet    ← fine, no datetime cols
  bronze/credit_bureau_reports/...parquet   ← fine, no datetime cols

FIX APPROACH:
  Download each affected Parquet file from S3
  → Convert all datetime/timestamp columns to STRING (ISO format)
  → Re-save with pyarrow using coerce_timestamps="ms" (microseconds)
  → Re-upload to S3, overwriting the bad files

WHY CONVERT TO STRING:
  The safest approach for Glue compatibility is to store all
  datetime columns as ISO 8601 strings ("2025-01-15T10:30:00")
  instead of Parquet TIMESTAMP type. Glue reads them as StringType,
  which works perfectly. Silver layer transforms them as needed.

Run: python fix_timestamp_parquet_files.py
=============================================================================
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — update bucket name to match yours
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"          # ← your actual bucket name


# ─────────────────────────────────────────────────────────────
# WHICH FILES TO FIX AND WHICH COLUMNS HAVE TIMESTAMPS
# ─────────────────────────────────────────────────────────────
FIX_CONFIGS = [
    {
        "prefix":           "bronze/branches/",
        "description":      "Branches — fix created_at timestamp",
        "timestamp_cols":   ["created_at"],
        "expected_rows":    5,
    },
    {
        "prefix":           "bronze/customers/",
        "description":      "Customers — fix created_at, updated_at timestamps",
        "timestamp_cols":   ["created_at", "updated_at"],
        "expected_rows":    100,
    },
    {
        "prefix":           "bronze/accounts/",
        "description":      "Accounts — fix created_at, updated_at timestamps",
        "timestamp_cols":   ["created_at", "updated_at"],
        "expected_rows":    100,
    },
    {
        "prefix":           "bronze/transactions/",
        "description":      "Transactions — fix txn_timestamp, created_at",
        "timestamp_cols":   ["txn_timestamp", "created_at"],
        "expected_rows":    1000,
    },
]


def list_parquet_files(s3, bucket, prefix):
    """List all Parquet files in an S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj["Key"])
    return sorted(files)


def download_parquet(s3, bucket, key) -> pd.DataFrame:
    """Download a Parquet file from S3 into a pandas DataFrame."""
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    # Use engine="fastparquet" as fallback if pyarrow fails on NANOS
    try:
        return pd.read_parquet(buf, engine="pyarrow")
    except Exception:
        buf.seek(0)
        return pd.read_parquet(buf, engine="fastparquet")


def convert_timestamps_to_string(df: pd.DataFrame, timestamp_cols: list) -> pd.DataFrame:
    """
    Convert datetime/timestamp columns to ISO 8601 string format.
    This makes them readable by ANY version of Spark/Glue/Athena.

    BEFORE: created_at = Timestamp('2025-01-15 10:30:00.123456789') [NANOS]
    AFTER:  created_at = "2025-01-15T10:30:00.123456"              [string]
    """
    df = df.copy()
    for col in timestamp_cols:
        if col not in df.columns:
            print(f"    [SKIP] Column '{col}' not found in DataFrame")
            continue

        original_dtype = str(df[col].dtype)

        # Handle different input types
        if "datetime" in original_dtype or "timestamp" in original_dtype:
            # Already a datetime type — convert to string
            df[col] = df[col].apply(
                lambda x: x.isoformat() if pd.notna(x) and hasattr(x, 'isoformat')
                else (str(x) if pd.notna(x) else None)
            )
        elif original_dtype == "object":
            # Already string — try to parse and re-format to ensure consistent format
            def safe_format(val):
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return None
                try:
                    dt = pd.to_datetime(val)
                    return dt.isoformat()
                except Exception:
                    return str(val)
            df[col] = df[col].apply(safe_format)
        else:
            print(f"    [INFO] Column '{col}' has dtype {original_dtype} — converting to string")
            df[col] = df[col].astype(str)

        print(f"    Converted '{col}': {original_dtype} → string")
        if len(df) > 0:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else "NULL"
            print(f"    Sample value: {sample}")

    return df


def also_fix_load_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """
    The load_timestamp column added by Phase 3 metadata might also be
    a datetime type. Convert it to string too.
    """
    meta_datetime_cols = ["load_timestamp"]
    for col in meta_datetime_cols:
        if col in df.columns:
            dtype = str(df[col].dtype)
            if "datetime" in dtype or "timestamp" in dtype:
                df[col] = df[col].apply(
                    lambda x: x.isoformat() if pd.notna(x) and hasattr(x, 'isoformat')
                    else str(x) if pd.notna(x) else None
                )
                print(f"    Also fixed metadata col '{col}': {dtype} → string")
    return df


def upload_fixed_parquet(df: pd.DataFrame, s3, bucket: str, key: str):
    """
    Save DataFrame as Parquet with Glue-compatible settings:
    - coerce_timestamps="ms"  → forces MICROSECOND precision (not NANOS)
    - allow_truncated_timestamps=True → prevents errors on truncation
    - compression="snappy"
    """
    # Convert to PyArrow table
    # Use schema inference — all timestamp-like cols are now strings
    # so no TIMESTAMP type will appear in the schema
    table = pa.Table.from_pandas(df, preserve_index=False)

    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression="snappy",
        use_dictionary=True,
        write_statistics=True,
        # These two settings prevent NANOS timestamp issues
        coerce_timestamps="ms",              # ← KEY FIX: use milliseconds
        allow_truncated_timestamps=True,     # ← allows ms truncation without error
    )
    buf.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
        Metadata={
            "fixed":           "timestamp-nanos-to-string",
            "fix-date":        datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "data-zone":       "bronze",
            "glue-compatible": "true",
        }
    )


def verify_fixed_file(s3, bucket, key):
    """
    Read the fixed file back and confirm:
    1. Row count is correct
    2. No TIMESTAMP type columns remain (all should be string)
    """
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    df = pd.read_parquet(buf)

    timestamp_cols_remaining = [
        col for col in df.columns
        if "datetime" in str(df[col].dtype) or "timestamp" in str(df[col].dtype)
    ]

    if timestamp_cols_remaining:
        print(f"    [WARN] Still has timestamp dtype cols: {timestamp_cols_remaining}")
        return False, len(df)
    else:
        print(f"    [OK]  No TIMESTAMP type cols — Glue compatible ✓")
        return True, len(df)


def main():
    print("=" * 65)
    print("  FIX: Regenerate Parquet files with Glue-compatible timestamps")
    print(f"  Bucket: s3://{BUCKET_NAME}")
    print(f"  Time  : {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)
    print("""
  ROOT CAUSE: Parquet files have INT64 (TIMESTAMP(NANOS,false)) columns.
  Glue Spark only supports TIMESTAMP(MICROS).
  FIX: Convert all datetime columns to ISO 8601 strings before saving.
    """)

    s3      = boto3.client("s3", region_name=AWS_REGION)
    results = []

    for config in FIX_CONFIGS:
        prefix = config["prefix"]
        print(f"\n{'─'*65}")
        print(f"  Table : {prefix}")
        print(f"  Desc  : {config['description']}")
        print(f"  Cols  : {config['timestamp_cols']}")

        # Find Parquet files in this prefix
        files = list_parquet_files(s3, BUCKET_NAME, prefix)
        if not files:
            print(f"  [SKIP] No Parquet files found in {prefix}")
            print(f"         Run Phase 3 step4_extract_rds_to_s3.py first")
            results.append({"prefix": prefix, "status": "SKIP — no files"})
            continue

        print(f"  Found : {len(files)} Parquet file(s)")
        for f in files:
            print(f"    - {f.split('/')[-1]}")

        for key in files:
            print(f"\n  [A] Downloading: {key.split('/')[-1]}...")
            try:
                df = download_parquet(s3, BUCKET_NAME, key)
                print(f"      Rows: {len(df):,}  |  Cols: {len(df.columns)}")
                print(f"      Current dtypes of target cols:")
                for col in config["timestamp_cols"]:
                    if col in df.columns:
                        print(f"        {col}: {df[col].dtype}")

            except Exception as e:
                print(f"  [FAIL] Could not download/read: {e}")
                print(f"         The file is corrupted or unreadable.")
                print(f"         You may need to re-run Phase 3 step4 to regenerate it.")
                results.append({"prefix": prefix, "status": f"DOWNLOAD FAILED: {e}"})
                continue

            print(f"\n  [B] Converting timestamp columns to strings...")
            df = convert_timestamps_to_string(df, config["timestamp_cols"])
            df = also_fix_load_timestamp(df)

            print(f"\n  [C] Re-uploading with Glue-compatible settings...")
            try:
                upload_fixed_parquet(df, s3, BUCKET_NAME, key)
                print(f"      [OK] Uploaded: s3://{BUCKET_NAME}/{key}")
            except Exception as e:
                print(f"  [FAIL] Upload failed: {e}")
                results.append({"prefix": prefix, "status": f"UPLOAD FAILED: {e}"})
                continue

            print(f"\n  [D] Verifying fixed file...")
            ok, row_count = verify_fixed_file(s3, BUCKET_NAME, key)
            print(f"      Row count: {row_count:,}")

            if row_count < config["expected_rows"]:
                print(f"      [WARN] Expected >= {config['expected_rows']:,} rows")

            results.append({
                "prefix":    prefix,
                "key":       key.split("/")[-1],
                "rows":      row_count,
                "status":    "FIXED ✓" if ok else "NEEDS REVIEW",
            })

    # ── Final Summary ────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  FIX SUMMARY")
    print(f"{'='*65}")
    print(f"\n  {'Table':<35} {'Status':<15} {'Rows':>8}")
    print(f"  {'-'*35} {'-'*15} {'-'*8}")
    for r in results:
        table  = r["prefix"].replace("bronze/","").replace("/","")
        status = r["status"]
        rows   = r.get("rows", 0)
        print(f"  {table:<35} {status:<15} {rows:>8,}")

    all_fixed = all("FIXED" in r["status"] or "SKIP" in r["status"] for r in results)

    print(f"\n{'='*65}")
    if all_fixed:
        print(f"  ALL FILES FIXED SUCCESSFULLY")
        print(f"\n  NEXT STEPS:")
        print(f"  1. Delete all existing Glue catalog tables for bronze_")
        print(f"     Glue → Data Catalog → Tables → select all → Delete")
        print(f"  2. Re-run Glue Crawler on bronze/ zone")
        print(f"     (with exclusion patterns **/.keep, *historical*, *incremental*)")
        print(f"  3. Verify 6 clean tables created in catalog")
        print(f"  4. Create the 6 Glue Visual ETL jobs from Phase 4 README")
        print(f"  5. When creating Source node in Visual ETL:")
        print(f"     → Data format: Parquet")
        print(f"     → The timestamp columns will show as StringType — this is correct")
    else:
        print(f"  SOME FILES NEED ATTENTION — check [FAIL] items above")
    print("=" * 65)


if __name__ == "__main__":
    main()
