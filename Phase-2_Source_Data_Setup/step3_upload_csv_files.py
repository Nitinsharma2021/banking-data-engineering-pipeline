"""
=============================================================================
PHASE 2 — STEP 3: UPLOAD CSV SOURCE FILES TO S3 LANDING ZONE
=============================================================================
Purpose  : Upload your 4 CSV files to the S3 landing/ zone
           These are the raw source files — the starting point of the pipeline
Run      : python step3_upload_csv_files.py
Expected : 4 files uploaded to s3://ameriprise-bank-datalake/landing/

YOUR 4 CSV FILES:
  payment_gateway_logs_1.csv              → 15,000 rows (historical)
  payment_gateway_logs_2_incremental.csv  →  5,000 rows (incremental Feb 2025)
  credit_bureau_reports_1.csv             →  4,000 rows (historical)
  credit_bureau_reports_2_incremental.csv →  1,500 rows (incremental Feb 2025)

WHAT IS THE LANDING ZONE?
  Think of landing/ as your inbox for incoming files.
  External vendors (payment gateways, credit bureaus) drop files here.
  Your pipeline picks them up, processes them, then moves to bronze/.
  The landing/ files are auto-deleted after 30 days (lifecycle rule set in step2).
=============================================================================
"""

import boto3
import os
import sys
from botocore.exceptions import ClientError
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"

# ── Tell the script where your CSV files are ──────────────────
# Change this path to wherever you saved the 4 CSV files on your laptop.
# If the CSVs are in the same folder as this script, leave it as "."
LOCAL_CSV_FOLDER = "/home/shreyansh-jain/Documents/Ameriprise_bank_project/phase2_complete_package/phase2_guide/Blob_Vendor_Data"       # e.g. "C:/Users/YourName/Downloads" on Windows
                              #       "/Users/yourname/Downloads" on Mac

# ── Mapping: local file name → S3 destination path ────────────
# Format: ("local_filename", "s3_prefix_folder", "file_type_description")
CSV_FILES = [
    (
        "payment_gateway_logs_1.csv",
        "landing/payment_gateway/",
        "Payment Gateway Logs — Historical (Jan 2024 – Jan 2025)",
        {"source": "payment_gateway", "load_type": "historical", "rows": "15000"}
    ),
    (
        "payment_gateway_logs_2_incremental.csv",
        "landing/payment_gateway/",
        "Payment Gateway Logs — Incremental (Feb 2025)",
        {"source": "payment_gateway", "load_type": "incremental", "rows": "5000"}
    ),
    (
        "credit_bureau_reports_1.csv",
        "landing/credit_bureau/",
        "Credit Bureau Reports — Historical (Jan 2025)",
        {"source": "credit_bureau", "load_type": "historical", "rows": "4000"}
    ),
    (
        "credit_bureau_reports_2_incremental.csv",
        "landing/credit_bureau/",
        "Credit Bureau Reports — Incremental (Feb 2025)",
        {"source": "credit_bureau", "load_type": "incremental", "rows": "1500"}
    ),
]


def get_file_size_mb(filepath):
    """Return file size in MB as a formatted string."""
    size_bytes = os.path.getsize(filepath)
    size_mb    = size_bytes / (1024 * 1024)
    return f"{size_mb:.2f} MB"


def upload_with_metadata(s3, local_path, bucket, s3_key, extra_tags):
    """
    Upload a file to S3 with:
    1. Progress display
    2. S3 object metadata (who uploaded, when, what it is)
    3. Server-side encryption tag
    """
    file_size = os.path.getsize(local_path)

    # S3 object metadata (visible in AWS Console → Object Properties)
    metadata = {
        "project":        "ameriprise-bank-de-pipeline",
        "pipeline-phase": "phase2-s3-setup",
        "upload-time":    datetime.utcnow().isoformat() + "Z",
        "data-zone":      "landing",
        "environment":    "development",
    }
    metadata.update(extra_tags)

    # Simple progress callback
    uploaded = [0]
    def progress(chunk):
        uploaded[0] += chunk
        pct = (uploaded[0] / file_size) * 100 if file_size > 0 else 100
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        print(f"\r    [{bar}] {pct:.1f}%  ({uploaded[0]:,} / {file_size:,} bytes)", end="", flush=True)

    s3.upload_file(
        Filename  = local_path,
        Bucket    = bucket,
        Key       = s3_key,
        ExtraArgs = {
            "Metadata":            metadata,
            "ServerSideEncryption": "AES256",
        },
        Callback  = progress
    )
    print()  # newline after progress bar


def verify_upload(s3, bucket, s3_key):
    """After upload, confirm the file exists in S3 and return its size."""
    resp = s3.head_object(Bucket=bucket, Key=s3_key)
    size_kb = resp["ContentLength"] / 1024
    return resp["ContentLength"], resp["LastModified"]


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 2 Step 3: Upload CSVs to S3 Landing")
    print("=" * 65)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    upload_date = datetime.utcnow().strftime("%Y%m%d")
    summary     = []
    errors      = []

    for filename, s3_prefix, description, tags in CSV_FILES:
        local_path = os.path.join(LOCAL_CSV_FOLDER, filename)

        print(f"\n  File   : {filename}")
        print(f"  Desc   : {description}")

        # ── Check file exists locally ───────────────────────────
        if not os.path.exists(local_path):
            print(f"  [ERROR] File not found: {local_path}")
            print(f"          Update LOCAL_CSV_FOLDER at the top of this script")
            print(f"          to point to the folder where your CSV files are saved")
            errors.append(filename)
            continue

        file_size_mb = get_file_size_mb(local_path)
        print(f"  Size   : {file_size_mb}")

        # ── Build S3 destination key ────────────────────────────
        # Add upload date to filename so you can track when each batch arrived
        name_no_ext = filename.replace(".csv", "")
        s3_key      = f"{s3_prefix}{name_no_ext}_{upload_date}.csv"
        print(f"  S3 Key : s3://{BUCKET_NAME}/{s3_key}")

        # ── Upload ──────────────────────────────────────────────
        print(f"  Upload :")
        try:
            upload_with_metadata(s3, local_path, BUCKET_NAME, s3_key, tags)
            size_bytes, last_modified = verify_upload(s3, BUCKET_NAME, s3_key)
            print(f"  [OK]    Verified in S3 — {size_bytes:,} bytes at {last_modified}")
            summary.append({
                "file":      filename,
                "s3_key":    s3_key,
                "size_mb":   file_size_mb,
                "status":    "UPLOADED"
            })
        except ClientError as e:
            print(f"  [FAIL]  {e}")
            errors.append(filename)

    # ── Summary table ───────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  UPLOAD SUMMARY")
    print(f"{'='*65}")
    print(f"  {'File':<50} {'Status':<10} {'Size'}")
    print(f"  {'-'*50} {'-'*10} {'-'*8}")
    for row in summary:
        print(f"  {row['file']:<50} {row['status']:<10} {row['size_mb']}")

    if errors:
        print(f"\n  ERRORS ({len(errors)} files failed):")
        for e in errors:
            print(f"    - {e}")
        print(f"\n  Fix the errors above then re-run this script.")
    else:
        print(f"\n  All {len(summary)} files uploaded successfully!")
        print(f"\n  View in AWS Console:")
        print(f"  → S3 → {BUCKET_NAME} → landing/payment_gateway/")
        print(f"  → S3 → {BUCKET_NAME} → landing/credit_bureau/")
        print(f"\n  NEXT STEP: Run step4_csv_to_parquet.py")
    print("=" * 65)


if __name__ == "__main__":
    main()
