"""
=============================================================================
PHASE 2 — STEP 2: CREATE S3 BUCKET + ALL ZONE FOLDERS
=============================================================================
Purpose  : Create 1 S3 bucket with all Bronze / Silver / Gold / Quarantine
           / Metadata sub-folders (called "prefixes" in S3)
Run      : python step2_create_s3_bucket.py
Expected : Bucket created + all 25 folders confirmed in S3
=============================================================================

WHAT THIS SCRIPT DOES:
  1. Creates the main S3 bucket: ameriprise-bank-datalake
  2. Enables versioning (keeps history of all file changes)
  3. Enables server-side encryption (AES256 — free, protects data at rest)
  4. Creates all zone prefixes (folders) for every source table
  5. Prints a tree view of everything created

S3 FOLDER STRUCTURE EXPLAINED:
  bronze/   ← Raw data exactly as it came from source (never change this)
  silver/   ← Cleaned, masked, standardised data (DQ passed)
  gold/     ← Analytics-ready star schema (dimensions + facts)
  quarantine/ ← Records that FAILED DQ checks (for investigation)
  metadata/ ← Watermarks, catalog, run logs
  landing/  ← Drop zone for incoming CSV files from external vendors
=============================================================================
"""

import boto3
import json
from botocore.exceptions import ClientError
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Change only these values if needed
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"                   # Mumbai (change if you prefer different region)
BUCKET_NAME = "neo-bank-datalake"     # Must be globally unique across ALL of AWS

# ─────────────────────────────────────────────────────────────
# ALL S3 FOLDERS TO CREATE
# We create a dummy placeholder file (.keep) in each folder
# because S3 doesn't actually have "folders" — they are just
# key prefixes. The .keep file makes the folder visible in Console.
# ─────────────────────────────────────────────────────────────
FOLDERS = [
    # ── LANDING ZONE (raw CSV files dropped here by vendors) ──
    "landing/payment_gateway/",
    "landing/credit_bureau/",

    # ── BRONZE ZONE (raw Parquet copies, never modified) ──────
    "bronze/branches/",
    "bronze/customers/",
    "bronze/accounts/",
    "bronze/transactions/",
    "bronze/payment_gateway_logs/",
    "bronze/credit_bureau_reports/",

    # ── SILVER ZONE (cleaned + masked Parquet) ─────────────────
    "silver/branches/",
    "silver/customers/",
    "silver/accounts/",
    "silver/transactions/",
    "silver/payment_gateway_logs/",
    "silver/credit_bureau_reports/",

    # ── GOLD ZONE (star schema: dimensions + facts + aggregates)
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

    # ── QUARANTINE ZONE (DQ-failed records) ───────────────────
    "quarantine/customers/",
    "quarantine/accounts/",
    "quarantine/transactions/",
    "quarantine/payment_gateway_logs/",
    "quarantine/credit_bureau_reports/",

    # ── METADATA ZONE (watermarks, audit logs, catalog) ───────
    "metadata/watermarks/",
    "metadata/catalog/",
    "metadata/run_logs/",
    "metadata/dq_results/",
]


def create_bucket(s3, bucket_name, region):
    """Create the S3 bucket. Handles the case where it already exists."""
    print(f"\n[1] Creating S3 bucket: {bucket_name}")
    try:
        if region == "us-east-1":
            # us-east-1 does NOT accept LocationConstraint (AWS quirk)
            s3.create_bucket(Bucket=bucket_name)
        else:
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region}
            )
        print(f"    [CREATED]  s3://{bucket_name}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"    [EXISTS]   s3://{bucket_name}  (already exists — that is fine)")
        else:
            raise e


def enable_versioning(s3, bucket_name):
    """Turn on versioning. This keeps ALL historical versions of every file."""
    print(f"\n[2] Enabling versioning on bucket...")
    s3.put_bucket_versioning(
        Bucket=bucket_name,
        VersioningConfiguration={"Status": "Enabled"}
    )
    print(f"    [ENABLED]  Versioning ON — every file change is preserved")
    print(f"               Benefit: You can recover any file from any point in time")


def enable_encryption(s3, bucket_name):
    """Enable server-side AES256 encryption (free, automatic, protects at rest)."""
    print(f"\n[3] Enabling server-side encryption (AES256)...")
    s3.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            "Rules": [{
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                }
            }]
        }
    )
    print(f"    [ENABLED]  AES256 encryption ON — all data encrypted at rest (free)")


def block_public_access(s3, bucket_name):
    """Block all public access — banking data must NEVER be public."""
    print(f"\n[4] Blocking all public access (important for banking data)...")
    s3.put_public_access_block(
        Bucket=bucket_name,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls":       True,
            "IgnorePublicAcls":      True,
            "BlockPublicPolicy":     True,
            "RestrictPublicBuckets": True
        }
    )
    print(f"    [BLOCKED]  Public access: OFF — bucket is private")


def create_folders(s3, bucket_name, folders):
    """Create all zone folders by uploading a tiny .keep placeholder file."""
    print(f"\n[5] Creating {len(folders)} zone folders...")
    created  = []
    existing = []

    placeholder_content = (
        "# This file marks the folder as created.\n"
        "# Do not delete it — it keeps the folder visible in AWS Console.\n"
        f"# Created: {datetime.utcnow().isoformat()}Z\n"
        "# Project: Ameriprise Bank Data Engineering Pipeline\n"
    ).encode("utf-8")

    for folder in folders:
        key = folder + ".keep"
        try:
            # Check if already exists
            s3.head_object(Bucket=bucket_name, Key=key)
            existing.append(folder)
            print(f"    [EXISTS]   s3://{bucket_name}/{folder}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                s3.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=placeholder_content,
                    ContentType="text/plain"
                )
                created.append(folder)
                print(f"    [CREATED]  s3://{bucket_name}/{folder}")
            else:
                raise e

    return created, existing


def add_lifecycle_policy(s3, bucket_name):
    """
    Add lifecycle rules to manage storage costs:
    - landing/ files deleted after 30 days (they are converted to Parquet)
    - quarantine/ files moved to cheaper storage after 90 days
    """
    print(f"\n[6] Adding lifecycle rules (cost management)...")
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            "Rules": [
                {
                    "ID":     "DeleteLandingAfter30Days",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "landing/"},
                    "Expiration": {"Days": 30},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 7}
                },
                {
                    "ID":     "ArchiveQuarantineAfter90Days",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "quarantine/"},
                    "Transitions": [{
                        "Days":         90,
                        "StorageClass": "STANDARD_IA"   # cheaper storage tier
                    }]
                },
                {
                    "ID":     "ArchiveBronzeAfter365Days",
                    "Status": "Enabled",
                    "Filter": {"Prefix": "bronze/"},
                    "Transitions": [{
                        "Days":         365,
                        "StorageClass": "GLACIER"        # cheapest long-term storage
                    }]
                }
            ]
        }
    )
    print(f"    [CREATED]  landing/ files auto-deleted after 30 days")
    print(f"    [CREATED]  quarantine/ moved to STANDARD_IA after 90 days (cheaper)")
    print(f"    [CREATED]  bronze/ archived to GLACIER after 365 days (cheapest)")


def add_bucket_tags(s3, bucket_name):
    """Tag the bucket so you know what project it belongs to."""
    print(f"\n[7] Adding resource tags...")
    s3.put_bucket_tagging(
        Bucket=bucket_name,
        Tagging={
            "TagSet": [
                {"Key": "Project",     "Value": "AmerispriseBankDEPipeline"},
                {"Key": "Phase",       "Value": "Phase2-S3DataLake"},
                {"Key": "Environment", "Value": "Development"},
                {"Key": "Owner",       "Value": "DataEngineering"},
                {"Key": "CostCenter",  "Value": "Banking-Analytics"},
            ]
        }
    )
    print(f"    [TAGGED]   5 tags applied to bucket")


def print_tree(bucket_name, folders):
    """Print a visual folder tree of everything created."""
    print(f"\n{'='*60}")
    print(f"  S3 BUCKET STRUCTURE CREATED")
    print(f"{'='*60}")
    print(f"\n  s3://{bucket_name}/")

    zones = {}
    for f in folders:
        zone = f.split("/")[0]
        zones.setdefault(zone, []).append(f)

    zone_icons = {
        "landing":    "📥",
        "bronze":     "🟤",
        "silver":     "⚪",
        "gold":       "🟡",
        "quarantine": "🔴",
        "metadata":   "📋",
    }

    for zone, paths in sorted(zones.items()):
        icon = zone_icons.get(zone, "📁")
        print(f"  ├── {icon} {zone}/")
        for i, path in enumerate(paths):
            sub     = "/".join(path.split("/")[1:])
            is_last = i == len(paths) - 1
            prefix  = "│       └──" if is_last else "│       ├──"
            print(f"  {prefix} {sub}")

    print(f"\n  Total folders: {len(folders)}")
    print(f"{'='*60}")


def main():
    print("=" * 60)
    print("  AMERIPRISE BANK — Phase 2: Create S3 Data Lake")
    print("=" * 60)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    create_bucket(s3, BUCKET_NAME, AWS_REGION)
    enable_versioning(s3, BUCKET_NAME)
    enable_encryption(s3, BUCKET_NAME)
    block_public_access(s3, BUCKET_NAME)
    created, existing = create_folders(s3, BUCKET_NAME, FOLDERS)
    add_lifecycle_policy(s3, BUCKET_NAME)
    add_bucket_tags(s3, BUCKET_NAME)
    print_tree(BUCKET_NAME, FOLDERS)

    print(f"\n  SUMMARY")
    print(f"  -------")
    print(f"  Bucket  : s3://{BUCKET_NAME}")
    print(f"  Region  : {AWS_REGION}")
    print(f"  Folders : {len(created)} created, {len(existing)} already existed")
    print(f"\n  NEXT STEP: Run step3_upload_csv_files.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
