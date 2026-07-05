"""
=============================================================================
PHASE 3 — STEP 5: VERIFY COMPLETE BRONZE LAYER
=============================================================================
Purpose  : Full audit of everything in S3 bronze/ after Phase 3
           Reads each Parquet file back from S3 and validates content
Run      : python step5_verify_bronze_layer.py
Expected : All 6 bronze Parquet files present and verified
           (4 from RDS + 2 from CSV in Phase 2)
=============================================================================
"""

import boto3
import pandas as pd
import io
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"

# Expected Parquet files in bronze/ after Phase 2 + Phase 3
EXPECTED_BRONZE_FILES = {
    "bronze/branches/":               {"min_rows": 5,    "source": "RDS",      "pk": "branch_code"},
    "bronze/customers/":              {"min_rows": 100,  "source": "RDS",      "pk": "customer_id"},
    "bronze/accounts/":               {"min_rows": 100,  "source": "RDS",      "pk": "account_id"},
    "bronze/transactions/":           {"min_rows": 1000, "source": "RDS",      "pk": "txn_id"},
    "bronze/payment_gateway_logs/":   {"min_rows": 1000, "source": "CSV",      "pk": "txn_id"},
    "bronze/credit_bureau_reports/":  {"min_rows": 1000, "source": "CSV",      "pk": "customer_id"},
}

REQUIRED_METADATA_COLS = [
    "src_system", "batch_id", "row_hash",
    "load_timestamp", "load_date", "is_active", "pipeline_phase"
]


def ok(msg):   print(f"    [PASS] {msg}")
def fail(msg): print(f"    [FAIL] {msg}")
def info(msg): print(f"           {msg}")


def list_parquet_files(s3, bucket, prefix):
    """List all Parquet files under a given S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    files     = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
    return files


def read_parquet_from_s3(s3, bucket, key) -> pd.DataFrame:
    """Download and read a Parquet file from S3 into a DataFrame."""
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pd.read_parquet(buf)


def verify_bronze_prefix(s3, bucket, prefix, config):
    """Verify one bronze/ prefix has Parquet files with correct content."""
    files = list_parquet_files(s3, bucket, prefix)

    if not files:
        fail(f"No Parquet files found in {prefix}")
        info(f"Source: {config['source']}")
        if config["source"] == "RDS":
            info("Fix: Run step4_extract_rds_to_s3.py")
        else:
            info("Fix: Run Phase 2 step4 + step5 scripts")
        return False, 0

    total_rows   = 0
    all_ok_local = True

    for f in files:
        key     = f["Key"]
        size_kb = f["Size"] / 1024
        info(f"File: {key}  ({size_kb:.1f} KB)")

        try:
            df   = read_parquet_from_s3(s3, bucket, key)
            rows = len(df)
            cols = list(df.columns)
            total_rows += rows

            info(f"  Rows: {rows:,}  |  Columns: {len(cols)}")

            # Check metadata columns
            missing = [c for c in REQUIRED_METADATA_COLS if c not in cols]
            if not missing:
                info(f"  Metadata columns: all {len(REQUIRED_METADATA_COLS)} present")
            else:
                info(f"  [WARN] Missing metadata cols: {missing}")
                all_ok_local = False

            # Check row hash format (should be 32-char MD5)
            if "row_hash" in df.columns:
                sample = df["row_hash"].iloc[0]
                if len(str(sample)) == 32:
                    info(f"  row_hash: valid MD5 format")
                else:
                    info(f"  [WARN] row_hash format unexpected: {sample}")

            # Check is_active all = 1
            if "is_active" in df.columns:
                active_count = df["is_active"].sum()
                if active_count == rows:
                    info(f"  is_active: all {rows:,} rows = 1 (active)")
                else:
                    info(f"  [WARN] {rows - active_count} rows have is_active != 1")

            # Show sample src_system values
            if "src_system" in df.columns:
                systems = df["src_system"].unique().tolist()
                info(f"  src_system: {systems}")

        except Exception as e:
            fail(f"Could not read {key}: {e}")
            all_ok_local = False

    # Check minimum rows
    if total_rows >= config["min_rows"]:
        ok(f"{prefix}  →  {total_rows:,} total rows  ✓")
    else:
        fail(f"{prefix}  →  {total_rows:,} rows (expected >= {config['min_rows']:,})")
        all_ok_local = False

    return all_ok_local, total_rows


def check_watermark(s3, bucket):
    """Read and display the watermark.json to confirm it was updated."""
    print(f"\n[WATERMARK CHECK]")
    try:
        resp = s3.get_object(Bucket=bucket, Key="metadata/watermarks/watermark.json")
        doc  = json.loads(resp["Body"].read().decode("utf-8"))
        sources = doc.get("sources", {})

        print(f"\n  {'Source':<30} {'Status':<15} {'Last Run':<30} {'Rows'}")
        print(f"  {'-'*30} {'-'*15} {'-'*30} {'-'*8}")

        for src, info_data in sources.items():
            status   = info_data.get("last_run_status", "UNKNOWN")
            last_run = info_data.get("last_successful_run", "never")[:19]
            rows     = info_data.get("rows_last_loaded", 0)
            print(f"  {src:<30} {status:<15} {last_run:<30} {rows:>8,}")

    except ClientError:
        print(f"  [WARN] watermark.json not found or not updated")
        print(f"         Run step4_extract_rds_to_s3.py to populate watermarks")


def print_bronze_tree(s3, bucket):
    """Print the complete bronze/ folder tree with sizes."""
    print(f"\n[BRONZE ZONE TREE]")
    print(f"\n  s3://{bucket}/bronze/")
    paginator  = s3.get_paginator("list_objects_v2")
    total_size = 0
    total_files= 0

    pages = paginator.paginate(Bucket=bucket, Prefix="bronze/")
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if ".keep" in key:
                continue
            size_kb     = obj["Size"] / 1024
            total_size  += obj["Size"]
            total_files += 1
            # Show just the last 2 parts of the path for readability
            parts = key.split("/")
            indent = "  " * (len(parts) - 1)
            print(f"  {indent}├── {parts[-1]}  ({size_kb:.1f} KB)")

    print(f"\n  Total: {total_files} files, {total_size/1024:.1f} KB in bronze/")


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 3 Step 5: Bronze Layer Verification")
    print(f"  Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    print(f"\n[BRONZE PARQUET FILE VERIFICATION]")
    all_results  = []
    total_rows   = 0

    for prefix, config in EXPECTED_BRONZE_FILES.items():
        print(f"\n  Checking: {prefix}  (source: {config['source']})")
        ok_flag, rows = verify_bronze_prefix(s3, BUCKET_NAME, prefix, config)
        all_results.append((prefix, ok_flag))
        total_rows += rows

    check_watermark(s3, BUCKET_NAME)
    print_bronze_tree(s3, BUCKET_NAME)

    # Final report
    print(f"\n{'='*65}")
    print(f"  PHASE 3 VERIFICATION REPORT")
    print(f"{'='*65}")
    passed = 0
    for prefix, result in all_results:
        name   = prefix.replace("bronze/","").replace("/","")
        status = "PASS" if result else "FAIL"
        sym    = "✓" if result else "✗"
        print(f"  [{status}]  {sym}  {name}")
        if result:
            passed += 1

    print(f"\n  {passed}/{len(all_results)} bronze zones verified")
    print(f"  Total rows across all bronze files: {total_rows:,}")

    if passed == len(all_results):
        print(f"\n  PHASE 3 COMPLETE!")
        print(f"  All 6 bronze zone Parquet files are correct.")
        print(f"\n  YOUR FULL BRONZE LAYER:")
        print(f"  ├── branches/     ← from RDS (5 branches)")
        print(f"  ├── customers/    ← from RDS (500+ customers)")
        print(f"  ├── accounts/     ← from RDS (1000+ accounts)")
        print(f"  ├── transactions/ ← from RDS (30000+ transactions)")
        print(f"  ├── payment_gateway_logs/  ← from CSV (Phase 2)")
        print(f"  └── credit_bureau_reports/ ← from CSV (Phase 2)")
        print(f"\n  READY FOR PHASE 4: Glue Data Quality Engine")
    else:
        print(f"\n  Fix the [FAIL] items and re-run this verification")
    print("=" * 65)


if __name__ == "__main__":
    main()
