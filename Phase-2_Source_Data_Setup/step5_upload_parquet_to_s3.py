"""
=============================================================================
PHASE 2 — STEP 5: UPLOAD PARQUET FILES TO S3 BRONZE ZONE
=============================================================================
Purpose  : Upload the Parquet files created in Step 4 to the S3 bronze/ zone
           This completes the Bronze layer of your data lake
Run      : python step5_upload_parquet_to_s3.py
Expected : 4 Parquet files in s3://ameriprise-bank-datalake/bronze/...
           1 batch manifest JSON in s3://ameriprise-bank-datalake/metadata/

WHAT IS THE BRONZE ZONE?
  Bronze = raw, untouched copy of source data stored as Parquet
  It has your 8 metadata columns added (src_system, batch_id, row_hash etc.)
  But the CONTENT (business data) is exactly as it came from the source
  You NEVER transform business values in bronze — that is Silver's job
  Bronze is your permanent audit trail — you should never delete it

S3 KEY PATTERN:
  bronze/payment_gateway_logs/payment_gateway_logs_historical_2025-01-15.parquet
  bronze/credit_bureau_reports/credit_bureau_reports_historical_2025-01-15.parquet
  
  For future runs (incremental), files are simply added:
  bronze/payment_gateway_logs/payment_gateway_logs_incremental_2025-02-01.parquet
  (Old files stay — they are the audit history)
=============================================================================
"""

import boto3
import os
import json
import glob
from datetime import datetime
from botocore.exceptions import ClientError


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION         = "ap-south-1"
BUCKET_NAME        = "neo-bank-datalake"
LOCAL_PARQUET_DIR  = "./output_parquet"    # Output from step4

RUN_DATE = datetime.utcnow().strftime("%Y-%m-%d")


def find_parquet_files(base_dir: str):
    """
    Walk the output_parquet directory and find all .parquet files.
    Returns list of (local_path, relative_path) tuples.
    """
    files = []
    for root, dirs, filenames in os.walk(base_dir):
        for fname in filenames:
            if fname.endswith(".parquet"):
                local_path = os.path.join(root, fname)
                # relative path = the S3 key
                rel_path   = os.path.relpath(local_path, base_dir).replace("\\", "/")
                files.append((local_path, rel_path))
    return files


def get_parquet_stats(local_path: str) -> dict:
    """Quick read of a Parquet file to get row count and schema."""
    try:
        import pyarrow.parquet as pq
        pf       = pq.read_metadata(local_path)
        row_count = pf.num_rows
        columns  = [pf.row_group(0).column(i).path_in_schema
                    for i in range(pf.row_group(0).num_columns)]
        return {"row_count": row_count, "columns": columns, "num_columns": len(columns)}
    except Exception:
        return {"row_count": "unknown", "columns": [], "num_columns": 0}


def upload_parquet_file(s3, local_path: str, bucket: str, s3_key: str, stats: dict) -> bool:
    """Upload one Parquet file to S3 with metadata tags."""
    file_size = os.path.getsize(local_path)
    uploaded  = [0]

    def progress(chunk):
        uploaded[0] += chunk
        pct = (uploaded[0] / file_size) * 100 if file_size > 0 else 100
        bar = "█" * int(pct / 4) + "░" * (25 - int(pct / 4))
        print(f"\r    [{bar}] {pct:.0f}%", end="", flush=True)

    try:
        s3.upload_file(
            Filename=local_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={
                "Metadata": {
                    "project":         "ameriprise-bank-de-pipeline",
                    "data-zone":       "bronze",
                    "file-format":     "parquet",
                    "compression":     "snappy",
                    "row-count":       str(stats.get("row_count", "unknown")),
                    "upload-date":     RUN_DATE,
                    "pipeline-phase":  "phase2-s3-setup",
                },
                "ContentType":            "application/octet-stream",
                "ServerSideEncryption":   "AES256",
            },
            Callback=progress
        )
        print()  # newline after progress bar
        return True
    except ClientError as e:
        print(f"\n    [FAIL] {e}")
        return False


def verify_s3_object(s3, bucket: str, s3_key: str) -> dict:
    """Confirm the file exists in S3 and return its properties."""
    try:
        resp = s3.head_object(Bucket=bucket, Key=s3_key)
        return {
            "exists":        True,
            "size_bytes":    resp["ContentLength"],
            "last_modified": resp["LastModified"].isoformat(),
            "etag":          resp["ETag"].strip('"'),
        }
    except ClientError:
        return {"exists": False}


def upload_batch_manifest(s3, bucket: str, local_parquet_dir: str, upload_results: list):
    """
    Upload the batch manifest JSON to metadata/ zone.
    This tracks every file uploaded in this run.
    """
    manifest_files = glob.glob(os.path.join(local_parquet_dir, "batch_manifest_*.json"))
    if not manifest_files:
        print(f"\n  [SKIP] No batch manifest found in {local_parquet_dir}")
        return

    manifest_path = manifest_files[0]   # use the first (most recent)
    s3_key = f"metadata/run_logs/batch_manifest_{RUN_DATE}.json"

    # Append upload results to manifest
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["s3_uploads"] = upload_results
    manifest["bucket"]     = bucket

    # Save updated manifest
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    s3.upload_file(
        Filename=manifest_path,
        Bucket=bucket,
        Key=s3_key,
        ExtraArgs={"ContentType": "application/json", "ServerSideEncryption": "AES256"}
    )
    print(f"\n  Batch manifest → s3://{bucket}/{s3_key}")


def print_s3_tree(s3, bucket: str):
    """Print the bronze/ folder structure in S3 after uploading."""
    print(f"\n  Bronze zone contents in S3:")
    paginator = s3.get_paginator("list_objects_v2")
    pages     = paginator.paginate(Bucket=bucket, Prefix="bronze/")

    total_size  = 0
    total_files = 0

    for page in pages:
        for obj in page.get("Contents", []):
            key       = obj["Key"]
            size_kb   = obj["Size"] / 1024
            total_size  += obj["Size"]
            total_files += 1
            if not key.endswith(".keep"):
                print(f"    s3://{bucket}/{key}  ({size_kb:.1f} KB)")

    if total_files > 0:
        print(f"\n  Total: {total_files} files, {total_size/1024/1024:.2f} MB in bronze/")


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 2 Step 5: Upload Parquet → S3 Bronze")
    print("=" * 65)

    if not os.path.exists(LOCAL_PARQUET_DIR):
        print(f"\n  [ERROR] output_parquet folder not found: {LOCAL_PARQUET_DIR}")
        print(f"          Run step4_csv_to_parquet.py first!")
        return

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # Find all Parquet files to upload
    parquet_files = find_parquet_files(LOCAL_PARQUET_DIR)
    if not parquet_files:
        print(f"\n  [ERROR] No .parquet files found in {LOCAL_PARQUET_DIR}")
        print(f"          Run step4_csv_to_parquet.py first!")
        return

    print(f"\n  Found {len(parquet_files)} Parquet files to upload:")
    for lp, rp in parquet_files:
        print(f"    {rp}")

    upload_results = []

    for local_path, rel_path in parquet_files:
        s3_key = rel_path    # relative path becomes the S3 key directly
        print(f"\n{'─'*65}")
        print(f"  Uploading : {os.path.basename(local_path)}")
        print(f"  S3 Key    : s3://{BUCKET_NAME}/{s3_key}")

        # Get Parquet stats before upload
        stats = get_parquet_stats(local_path)
        print(f"  Rows      : {stats['row_count']:,}" if isinstance(stats['row_count'], int)
              else f"  Rows      : {stats['row_count']}")
        print(f"  Columns   : {stats['num_columns']}")
        print(f"  Local size: {os.path.getsize(local_path)/1024:.1f} KB")
        print(f"  Uploading :")

        success = upload_parquet_file(s3, local_path, BUCKET_NAME, s3_key, stats)

        if success:
            verify = verify_s3_object(s3, BUCKET_NAME, s3_key)
            if verify["exists"]:
                print(f"  [OK]  Verified in S3")
                print(f"        Size   : {verify['size_bytes']:,} bytes")
                print(f"        ETag   : {verify['etag'][:16]}...")
                upload_results.append({
                    "s3_key":   s3_key,
                    "status":   "UPLOADED",
                    "size":     verify["size_bytes"],
                    "rows":     stats["row_count"],
                })
            else:
                print(f"  [WARN] File uploaded but verification failed")
        else:
            upload_results.append({"s3_key": s3_key, "status": "FAILED"})

    # Upload manifest to metadata/
    print(f"\n{'─'*65}")
    print(f"  Uploading batch manifest to metadata/...")
    upload_batch_manifest(s3, BUCKET_NAME, LOCAL_PARQUET_DIR, upload_results)

    # Print what's in bronze/
    print_s3_tree(s3, BUCKET_NAME)

    # Summary
    ok    = sum(1 for r in upload_results if r["status"] == "UPLOADED")
    fail  = sum(1 for r in upload_results if r["status"] == "FAILED")
    print(f"\n{'='*65}")
    print(f"  SUMMARY: {ok} uploaded, {fail} failed")
    print(f"\n  NEXT STEP: Run step6_verify_s3_structure.py")
    print("=" * 65)


if __name__ == "__main__":
    main()
