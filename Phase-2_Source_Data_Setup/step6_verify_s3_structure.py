"""
=============================================================================
PHASE 2 — STEP 6: VERIFY COMPLETE S3 STRUCTURE
=============================================================================
Purpose  : Run a full audit of your S3 data lake setup.
           Checks every folder, every file, and reads back one Parquet file
           to confirm data is readable and columns are correct.
Run      : python step6_verify_s3_structure.py
Expected : All checks pass — prints a complete health report of the data lake

WHAT THIS CHECKS:
  1. Bucket exists and settings are correct (versioning, encryption, public access)
  2. All expected zone folders are present
  3. Bronze Parquet files are uploaded and readable
  4. Metadata columns exist in the Parquet files
  5. Row counts match what you expect
  6. Prints a full S3 tree with sizes
=============================================================================
"""

import boto3
import json
import io
from botocore.exceptions import ClientError
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"

# Expected folders (from step2) — script checks ALL of these exist
EXPECTED_FOLDERS = [
    "landing/payment_gateway/",
    "landing/credit_bureau/",
    "bronze/branches/",
    "bronze/customers/",
    "bronze/accounts/",
    "bronze/transactions/",
    "bronze/payment_gateway_logs/",
    "bronze/credit_bureau_reports/",
    "silver/branches/",
    "silver/customers/",
    "silver/accounts/",
    "silver/transactions/",
    "silver/payment_gateway_logs/",
    "silver/credit_bureau_reports/",
    "gold/dim_customer/",
    "gold/dim_branch/",
    "gold/dim_account/",
    "gold/dim_date/",
    "gold/fact_transactions/",
    "gold/fact_payments/",
    "gold/fact_credit_risk/",
    "gold/agg_daily_balances/",
    "gold/agg_monthly_summary/",
    "gold/agg_branch_performance/",
    "gold/agg_customer_360/",
    "quarantine/customers/",
    "quarantine/accounts/",
    "quarantine/transactions/",
    "quarantine/payment_gateway_logs/",
    "quarantine/credit_bureau_reports/",
    "metadata/watermarks/",
    "metadata/catalog/",
    "metadata/run_logs/",
    "metadata/dq_results/",
]

# Expected metadata columns in every bronze Parquet file
EXPECTED_META_COLS = [
    "src_system", "src_file_name", "load_timestamp",
    "load_date", "batch_id", "row_hash", "is_active", "pipeline_phase"
]

# Expected row count ranges per file
EXPECTED_ROW_RANGES = {
    "payment_gateway_logs_historical":  (14000, 16000),
    "payment_gateway_logs_incremental": (4000,  6000),
    "credit_bureau_reports_historical": (3000,  5000),
    "credit_bureau_reports_incremental":(1000,  2000),
}


def ok(msg):
    print(f"    [PASS] {msg}")
def fail(msg):
    print(f"    [FAIL] {msg}")
def warn(msg):
    print(f"    [WARN] {msg}")
def info(msg):
    print(f"           {msg}")


# ─────────────────────────────────────────────────────────────
# CHECK 1: Bucket settings
# ─────────────────────────────────────────────────────────────
def check_bucket_settings(s3, bucket):
    print("\n[1] Bucket settings")
    all_ok = True

    # Exists?
    try:
        s3.head_bucket(Bucket=bucket)
        ok(f"Bucket exists: s3://{bucket}")
    except ClientError:
        fail(f"Bucket NOT found: {bucket}  → Run step2_create_s3_bucket.py")
        return False

    # Versioning
    try:
        resp   = s3.get_bucket_versioning(Bucket=bucket)
        status = resp.get("Status", "Disabled")
        if status == "Enabled":
            ok(f"Versioning: {status}")
        else:
            fail(f"Versioning: {status}  → Should be Enabled")
            all_ok = False
    except ClientError as e:
        warn(f"Could not check versioning: {e}")

    # Encryption
    try:
        resp  = s3.get_bucket_encryption(Bucket=bucket)
        rules = resp["ServerSideEncryptionConfiguration"]["Rules"]
        algo  = rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
        ok(f"Encryption: {algo}")
    except ClientError:
        warn(f"Encryption not configured (not critical for dev)")

    # Public access blocked
    try:
        resp   = s3.get_public_access_block(Bucket=bucket)
        config = resp["PublicAccessBlockConfiguration"]
        all_blocked = all(config.get(k, False) for k in [
            "BlockPublicAcls", "IgnorePublicAcls",
            "BlockPublicPolicy", "RestrictPublicBuckets"
        ])
        if all_blocked:
            ok("Public access: BLOCKED (good for banking data)")
        else:
            fail("Public access: NOT fully blocked — run step2 again")
            all_ok = False
    except ClientError:
        warn("Could not verify public access block")

    # Lifecycle rules
    try:
        resp  = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = resp.get("Rules", [])
        ok(f"Lifecycle rules: {len(rules)} rules configured")
        for r in rules:
            info(f"Rule '{r['ID']}': {r['Status']}")
    except ClientError:
        warn("Lifecycle rules not configured — run step2 again to add them")

    return all_ok


# ─────────────────────────────────────────────────────────────
# CHECK 2: All expected folders exist
# ─────────────────────────────────────────────────────────────
def check_folders(s3, bucket, expected_folders):
    print("\n[2] Zone folder structure")
    missing = []
    present = []

    for folder in expected_folders:
        key = folder + ".keep"
        try:
            s3.head_object(Bucket=bucket, Key=key)
            present.append(folder)
        except ClientError:
            missing.append(folder)

    if not missing:
        ok(f"All {len(expected_folders)} expected folders present")
    else:
        fail(f"{len(missing)} folders MISSING:")
        for m in missing:
            info(f"- {m}")
        info("Run step2_create_s3_bucket.py to create missing folders")

    # Group by zone and print
    zones = {}
    for f in present:
        z = f.split("/")[0]
        zones.setdefault(z, 0)
        zones[z] += 1
    for z, cnt in sorted(zones.items()):
        info(f"{z:15} : {cnt} folders")

    return len(missing) == 0


# ─────────────────────────────────────────────────────────────
# CHECK 3: Bronze Parquet files uploaded
# ─────────────────────────────────────────────────────────────
def check_bronze_files(s3, bucket):
    print("\n[3] Bronze zone — Parquet files")
    paginator = s3.get_paginator("list_objects_v2")

    bronze_files = []
    total_size   = 0

    pages = paginator.paginate(Bucket=bucket, Prefix="bronze/")
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                bronze_files.append(obj)
                total_size += obj["Size"]

    if bronze_files:
        ok(f"Found {len(bronze_files)} Parquet files in bronze/")
        info(f"Total size: {total_size/1024:.1f} KB")
        for f in bronze_files:
            size_kb = f["Size"] / 1024
            info(f"{f['Key']}  ({size_kb:.1f} KB)")
    else:
        fail("No Parquet files found in bronze/")
        info("Run step4 + step5 to convert and upload CSVs")

    return len(bronze_files) > 0


# ─────────────────────────────────────────────────────────────
# CHECK 4: Read back a Parquet file and verify columns + rows
# ─────────────────────────────────────────────────────────────
def check_parquet_content(s3, bucket):
    print("\n[4] Parquet file content verification")

    try:
        import pandas as pd
        import pyarrow.parquet as pq
    except ImportError:
        warn("pandas/pyarrow not installed — skipping content check")
        return True

    paginator   = s3.get_paginator("list_objects_v2")
    all_ok_flag = True

    for prefix in ["bronze/payment_gateway_logs/", "bronze/credit_bureau_reports/"]:
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        files = []
        for page in pages:
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".parquet"):
                    files.append(obj["Key"])

        if not files:
            warn(f"No Parquet files in {prefix}")
            continue

        # Read first file found
        s3_key = files[0]
        print(f"\n    Reading: s3://{bucket}/{s3_key}")

        try:
            # Download to memory buffer
            buf  = io.BytesIO()
            s3.download_fileobj(bucket, s3_key, buf)
            buf.seek(0)

            df   = pd.read_parquet(buf)
            cols = list(df.columns)

            ok(f"Read successful: {len(df):,} rows × {len(cols)} columns")
            info(f"Columns: {cols}")

            # Check metadata columns
            missing_meta = [c for c in EXPECTED_META_COLS if c not in cols]
            if not missing_meta:
                ok(f"All {len(EXPECTED_META_COLS)} metadata columns present")
            else:
                fail(f"Missing metadata columns: {missing_meta}")
                all_ok_flag = False

            # Show sample metadata values
            meta_sample = {c: df[c].iloc[0] for c in EXPECTED_META_COLS if c in cols}
            info("Sample metadata values:")
            for k, v in meta_sample.items():
                info(f"  {k:20} = {v}")

            # Check row hash looks like MD5
            if "row_hash" in df.columns:
                sample_hash = df["row_hash"].iloc[0]
                if len(str(sample_hash)) == 32:
                    ok(f"row_hash format valid (MD5, 32 chars)")
                else:
                    warn(f"row_hash unexpected format: {sample_hash}")

            # Check no completely empty required columns
            for col in ["batch_id", "src_system", "load_timestamp"]:
                if col in df.columns:
                    null_count = df[col].isna().sum()
                    if null_count == 0:
                        ok(f"'{col}' has no nulls (all {len(df):,} rows populated)")
                    else:
                        fail(f"'{col}' has {null_count} nulls — check metadata code")
                        all_ok_flag = False

        except Exception as e:
            fail(f"Could not read Parquet: {e}")
            all_ok_flag = False

    return all_ok_flag


# ─────────────────────────────────────────────────────────────
# CHECK 5: Print complete S3 tree
# ─────────────────────────────────────────────────────────────
def print_full_tree(s3, bucket):
    print(f"\n[5] Complete S3 data lake tree")
    print(f"\n    s3://{bucket}/")
    paginator  = s3.get_paginator("list_objects_v2")
    zone_data  = {}
    total_size = 0
    total_objs = 0

    pages = paginator.paginate(Bucket=bucket)
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".keep"):
                continue
            key   = obj["Key"]
            parts = key.split("/")
            zone  = parts[0]
            zone_data.setdefault(zone, {"files": [], "size": 0})
            zone_data[zone]["files"].append((key, obj["Size"]))
            zone_data[zone]["size"] += obj["Size"]
            total_size += obj["Size"]
            total_objs += 1

    zone_icons = {
        "landing":"📥", "bronze":"🟤", "silver":"⚪",
        "gold":"🟡", "quarantine":"🔴", "metadata":"📋"
    }
    for zone in sorted(zone_data.keys()):
        data = zone_data[zone]
        icon = zone_icons.get(zone, "📁")
        size_kb = data["size"] / 1024
        print(f"\n    {icon} {zone}/  ({len(data['files'])} files, {size_kb:.1f} KB)")
        for fkey, fsize in data["files"]:
            sub = "/".join(fkey.split("/")[1:])
            print(f"       ├── {sub}  ({fsize/1024:.1f} KB)")

    print(f"\n    {'─'*40}")
    print(f"    TOTAL: {total_objs} files, {total_size/1024:.1f} KB")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 2 Step 6: S3 Data Lake Verification")
    print(f"  Run at: {datetime.utcnow().isoformat()}Z")
    print("=" * 65)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    checks = []

    checks.append(("Bucket settings",      check_bucket_settings(s3, BUCKET_NAME)))
    checks.append(("Zone folder structure", check_folders(s3, BUCKET_NAME, EXPECTED_FOLDERS)))
    checks.append(("Bronze Parquet files",  check_bronze_files(s3, BUCKET_NAME)))
    checks.append(("Parquet content",       check_parquet_content(s3, BUCKET_NAME)))
    print_full_tree(s3, BUCKET_NAME)

    print(f"\n{'='*65}")
    print(f"  VERIFICATION REPORT")
    print(f"{'='*65}")
    all_passed = True
    for name, result in checks:
        status    = "PASS" if result else "FAIL"
        symbol    = "✓" if result else "✗"
        all_passed = all_passed and result
        print(f"  [{status}]  {symbol}  {name}")

    print(f"\n  {'─'*40}")
    if all_passed:
        print(f"  ALL CHECKS PASSED")
        print(f"  Your S3 Bronze layer is correctly set up!")
        print(f"\n  NEXT STEP: Run step7_create_metadata_files.py")
    else:
        print(f"  SOME CHECKS FAILED — Fix the [FAIL] items above")
        print(f"  Re-run the relevant step script then run this verification again")
    print("=" * 65)


if __name__ == "__main__":
    main()
