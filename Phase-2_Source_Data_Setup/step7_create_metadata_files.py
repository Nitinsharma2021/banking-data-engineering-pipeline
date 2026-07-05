"""
=============================================================================
PHASE 2 — STEP 7: CREATE METADATA FILES IN S3
=============================================================================
Purpose  : Create 3 critical metadata files in the metadata/ zone:
           1. watermark.json     — tracks "last time each source was loaded"
           2. data_catalog.json  — describes every table and column
           3. pipeline_config.json — pipeline settings used in Phase 4+
Run      : python step7_create_metadata_files.py
Expected : 3 JSON files in s3://ameriprise-bank-datalake/metadata/

WHY METADATA FILES MATTER:
  watermark.json:
    Without this, your Phase 4 incremental pipeline doesn't know
    "which records are new since last run". The watermark stores the
    last_run_timestamp per source table. Phase 4 reads this file,
    compares it to updated_at/txn_timestamp, and only pulls NEW rows.

  data_catalog.json:
    Documents every column in every table — what it means, its data type,
    whether it's PII, what its valid values are. This is your data dictionary.
    Required for governance and for future team members to understand the data.

  pipeline_config.json:
    Central configuration for the whole pipeline — bucket name, region,
    thresholds, schedule. All pipeline scripts read from this instead of
    having hardcoded values in every script.
=============================================================================
"""

import boto3
import json
from datetime import datetime, timezone


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"

# The "epoch" watermark — use this for first-ever run.
# Any record with updated_at > 1900-01-01 will be picked up = full load.
EPOCH_WATERMARK = "1900-01-01T00:00:00Z"
NOW             = datetime.now(timezone.utc).isoformat()


def upload_json(s3, bucket, s3_key, data, description):
    """Upload a Python dict as a JSON file to S3."""
    body = json.dumps(data, indent=2, default=str).encode("utf-8")

    # Clean description — remove non-ASCII characters for S3 metadata
    clean_description = description.encode("ascii", errors="ignore").decode("ascii")

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=body,
        ContentType="application/json",
       ServerSideEncryption="AES256",
        Metadata={
            "project":     "ameriprise-bank-de-pipeline",
            "data-zone":   "metadata",
            "description": clean_description,
        }
    )
    
    size_kb = len(body) / 1024
    print(f"  [OK]  s3://{bucket}/{s3_key}  ({size_kb:.1f} KB)")
    return True


# ─────────────────────────────────────────────────────────────
# FILE 1: WATERMARK TABLE
# ─────────────────────────────────────────────────────────────
def build_watermark():
    """
    Watermark = last successful load timestamp per source.
    Phase 4 reads this to know "only pull records newer than this".

    For Phase 2 (first ever run), all watermarks are set to epoch (1900-01-01).
    After Phase 4 runs, it will UPDATE this file with the actual run timestamps.

    Structure:
      {
        "source_name": {
          "last_successful_run": "YYYY-MM-DDThh:mm:ssZ",
          "last_run_status":     "NEVER_RUN" | "SUCCESS" | "FAILED",
          "rows_last_loaded":    0
        }
      }
    """
    return {
        "_meta": {
            "description":  "Watermark table — tracks last successful load per source",
            "created_at":   NOW,
            "last_updated": NOW,
            "managed_by":   "phase4_ingestion_pipeline",
            "note":         "Do not edit manually. Updated automatically after each pipeline run."
        },
        "sources": {
            # ── SQL Server sources (Phase 4 will extract from RDS) ──
            "banking.branches": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "FULL_MERGE",
                "watermark_column":    None,         # full load = no watermark column needed
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "notes":               "Small table, always full MERGE. No watermark needed."
            },
            "banking.customers": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "INCREMENTAL_MERGE",
                "watermark_column":    "updated_at",
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "notes":               "MERGE on customer_id. Pull where updated_at > last_run."
            },
            "banking.accounts": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "INCREMENTAL_MERGE",
                "watermark_column":    "updated_at",
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "notes":               "MERGE on account_id. Pull where updated_at > last_run."
            },
            "banking.transactions": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "APPEND_ONLY",
                "watermark_column":    "txn_timestamp",
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "notes":               "INSERT ONLY. Pull where txn_timestamp > last_run. Never update."
            },
            # ── CSV file sources ──────────────────────────────────
            "payment_gateway_csv": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "FILE_APPEND",
                "watermark_column":    "processed_timestamp",
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "files_processed":     [],
                "notes":               "File-based. Track by filename. New file = new batch."
            },
            "credit_bureau_csv": {
                "last_successful_run": EPOCH_WATERMARK,
                "last_run_status":     "NEVER_RUN",
                "load_type":           "INCREMENTAL_FILE",
                "watermark_column":    "bureau_pull_date",
                "rows_last_loaded":    0,
                "last_batch_id":       None,
                "files_processed":     [],
                "notes":               "Monthly pull. MERGE on (customer_id, bureau_pull_date)."
            }
        }
    }


# ─────────────────────────────────────────────────────────────
# FILE 2: DATA CATALOG
# ─────────────────────────────────────────────────────────────
def build_data_catalog():
    """
    Data catalog = documentation of every table and column.
    Includes: zone, description, data type, PII flag, valid values, etc.
    """
    return {
        "_meta": {
            "description":  "Ameriprise Bank data lake catalog — all tables and columns",
            "created_at":   NOW,
            "last_updated": NOW,
            "total_tables": 12,
        },
        "tables": {

            # ── BRONZE: payment_gateway_logs ─────────────────────
            "bronze.payment_gateway_logs": {
                "zone":        "bronze",
                "source":      "payment_gateway_csv",
                "description": "Raw payment gateway transaction logs from Stripe, BillDesk, PayU, Razorpay",
                "load_type":   "APPEND_ONLY",
                "row_count_approx": 20000,
                "columns": {
                    "txn_id":             {"type":"int64",   "nullable":False,"pii":False,"pk":True,  "description":"Unique transaction ID"},
                    "gateway_name":       {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Payment gateway name","valid_values":["Stripe","BillDesk","PayU","Razorpay"]},
                    "gateway_status":     {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Gateway processing status","valid_values":["SUCCESS","FAILED","PENDING","TIMEOUT"]},
                    "response_code":      {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"2-char response code. 00=success"},
                    "processing_time_ms": {"type":"int64",   "nullable":False,"pii":False,"pk":False, "description":"Processing time in milliseconds","valid_range":[0,10000]},
                    "device_type":        {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Device used for payment","valid_values":["Mobile","ATM","POS","Web"]},
                    "geo_location":       {"type":"string",  "nullable":True, "pii":False,"pk":False, "description":"City where transaction occurred"},
                    "processed_timestamp":{"type":"datetime","nullable":False,"pii":False,"pk":False, "description":"UTC timestamp of gateway processing"},
                    "src_system":         {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Pipeline: source system name","metadata":True},
                    "src_file_name":      {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Pipeline: original CSV filename","metadata":True},
                    "load_timestamp":     {"type":"datetime","nullable":False,"pii":False,"pk":False, "description":"Pipeline: UTC load time","metadata":True},
                    "load_date":          {"type":"date",    "nullable":False,"pii":False,"pk":False, "description":"Pipeline: load date (for partitioning)","metadata":True},
                    "batch_id":           {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Pipeline: UUID for this run","metadata":True},
                    "row_hash":           {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Pipeline: MD5 hash of source columns","metadata":True},
                    "is_active":          {"type":"int64",   "nullable":False,"pii":False,"pk":False, "description":"Pipeline: 1=active, 0=soft-deleted","metadata":True},
                    "pipeline_phase":     {"type":"string",  "nullable":False,"pii":False,"pk":False, "description":"Pipeline: data zone (bronze/silver/gold)","metadata":True},
                }
            },

            # ── BRONZE: credit_bureau_reports ────────────────────
            "bronze.credit_bureau_reports": {
                "zone":        "bronze",
                "source":      "credit_bureau_csv",
                "description": "Monthly credit bureau reports per customer (CIBIL-style data)",
                "load_type":   "INCREMENTAL_MERGE",
                "row_count_approx": 5500,
                "columns": {
                    "customer_id":             {"type":"int64",  "nullable":False,"pii":False, "pk":True,  "description":"Customer identifier (FK to customers table)"},
                    "credit_score":            {"type":"int64",  "nullable":False,"pii":False, "pk":False, "description":"Credit score 300–900. Higher = better creditworthiness","valid_range":[300,900]},
                    "risk_grade":              {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Risk classification","valid_values":["LOW","MEDIUM","HIGH"],"note":"LOW risk_grade = HIGH credit_score (counterintuitive naming)"},
                    "external_active_loans":   {"type":"int64",  "nullable":False,"pii":False, "pk":False, "description":"Number of active loans at other institutions","valid_range":[0,20]},
                    "external_overdue_amount": {"type":"float64","nullable":False,"pii":False, "pk":False, "description":"Total overdue amount in INR across external loans","valid_range":[0,500000]},
                    "bureau_pull_date":        {"type":"date",   "nullable":False,"pii":False, "pk":True,  "description":"Date credit bureau report was pulled (monthly)"},
                    "src_system":              {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "src_file_name":           {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "load_timestamp":          {"type":"datetime","nullable":False,"pii":False,"pk":False, "description":"Pipeline metadata","metadata":True},
                    "load_date":               {"type":"date",   "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "batch_id":                {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "row_hash":                {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "is_active":               {"type":"int64",  "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                    "pipeline_phase":          {"type":"string", "nullable":False,"pii":False, "pk":False, "description":"Pipeline metadata","metadata":True},
                }
            },

            # ── BRONZE: SQL Server sources (placeholders — filled in Phase 4) ──
            "bronze.branches": {
                "zone":"bronze","source":"banking_rds",
                "description":"Bank branches — 5 branches (BR001-BR005)",
                "load_type":"FULL_MERGE",
                "columns": {
                    "branch_code":  {"type":"string","nullable":False,"pii":False,"pk":True, "description":"Unique branch code e.g. BR001"},
                    "branch_name":  {"type":"string","nullable":False,"pii":False,"pk":False,"description":"Branch display name"},
                    "city":         {"type":"string","nullable":False,"pii":False,"pk":False,"description":"City where branch is located"},
                    "state":        {"type":"string","nullable":False,"pii":False,"pk":False,"description":"State where branch is located"},
                    "region":       {"type":"string","nullable":True, "pii":False,"pk":False,"description":"Region grouping (North/South/West/East)"},
                    "created_at":   {"type":"datetime","nullable":False,"pii":False,"pk":False,"description":"Record creation timestamp in source"},
                }
            },
            "bronze.customers": {
                "zone":"bronze","source":"banking_rds",
                "description":"Bank customers — ~500 customers",
                "load_type":"INCREMENTAL_MERGE",
                "columns": {
                    "customer_id":  {"type":"int64", "nullable":False,"pii":False,"pk":True, "description":"Unique customer ID"},
                    "first_name":   {"type":"string","nullable":False,"pii":True, "pk":False,"description":"Customer first name","classification":"PII-INTERNAL"},
                    "last_name":    {"type":"string","nullable":False,"pii":True, "pk":False,"description":"Customer last name","classification":"PII-INTERNAL"},
                    "date_of_birth":{"type":"date",  "nullable":False,"pii":True, "pk":False,"description":"Date of birth","classification":"PII-CONFIDENTIAL"},
                    "pan_number":   {"type":"string","nullable":True, "pii":True, "pk":False,"description":"PAN number (India tax ID, format: AAAAA9999A)","classification":"PII-CONFIDENTIAL"},
                    "email":        {"type":"string","nullable":True, "pii":True, "pk":False,"description":"Email address","classification":"PII-CONFIDENTIAL"},
                    "phone_number": {"type":"string","nullable":True, "pii":True, "pk":False,"description":"10-digit Indian mobile number","classification":"PII-CONFIDENTIAL"},
                    "kyc_status":   {"type":"string","nullable":False,"pii":False,"pk":False,"description":"KYC verification status","valid_values":["VERIFIED","PENDING","REJECTED"]},
                    "branch_code":  {"type":"string","nullable":False,"pii":False,"pk":False,"description":"FK to branches table"},
                    "created_at":   {"type":"datetime","nullable":False,"pii":False,"pk":False,"description":"Record creation timestamp"},
                    "updated_at":   {"type":"datetime","nullable":False,"pii":False,"pk":False,"description":"Last update timestamp (used as watermark)"},
                }
            },
            "bronze.accounts": {
                "zone":"bronze","source":"banking_rds",
                "description":"Customer bank accounts",
                "load_type":"INCREMENTAL_MERGE",
                "columns": {
                    "account_id":  {"type":"int64",  "nullable":False,"pii":False,"pk":True, "description":"Unique account ID"},
                    "customer_id": {"type":"int64",  "nullable":False,"pii":False,"pk":False,"description":"FK to customers"},
                    "account_type":{"type":"string", "nullable":False,"pii":False,"pk":False,"description":"Account type","valid_values":["Savings","Current"]},
                    "balance":     {"type":"float64","nullable":False,"pii":False,"pk":False,"description":"Current balance in INR","valid_range":[0,99999999]},
                    "currency":    {"type":"string", "nullable":False,"pii":False,"pk":False,"description":"Currency code","valid_values":["INR"]},
                    "branch_code": {"type":"string", "nullable":False,"pii":False,"pk":False,"description":"FK to branches"},
                    "status":      {"type":"string", "nullable":False,"pii":False,"pk":False,"description":"Account status","valid_values":["ACTIVE","CLOSED","FROZEN","DORMANT"]},
                    "opened_date": {"type":"date",   "nullable":False,"pii":False,"pk":False,"description":"Account opening date"},
                    "updated_at":  {"type":"datetime","nullable":False,"pii":False,"pk":False,"description":"Last update timestamp (watermark)"},
                }
            },
            "bronze.transactions": {
                "zone":"bronze","source":"banking_rds",
                "description":"All bank transactions — append only",
                "load_type":"APPEND_ONLY",
                "columns": {
                    "txn_id":       {"type":"int64",  "nullable":False,"pii":False,"pk":True, "description":"Unique transaction ID"},
                    "account_id":   {"type":"int64",  "nullable":False,"pii":False,"pk":False,"description":"FK to accounts"},
                    "txn_type":     {"type":"string", "nullable":False,"pii":False,"pk":False,"description":"Transaction type","valid_values":["Debit","Credit"]},
                    "amount":       {"type":"float64","nullable":False,"pii":False,"pk":False,"description":"Transaction amount in INR","valid_range":[0.01,9999999]},
                    "txn_timestamp":{"type":"datetime","nullable":False,"pii":False,"pk":False,"description":"Transaction timestamp (watermark)"},
                    "channel":      {"type":"string", "nullable":True, "pii":False,"pk":False,"description":"Channel used","valid_values":["UPI","ATM","NEFT","IMPS","RTGS","POS","Net Banking"]},
                    "status":       {"type":"string", "nullable":False,"pii":False,"pk":False,"description":"Transaction status","valid_values":["SUCCESS","FAILED","PENDING","REVERSED"]},
                }
            },
        }
    }


# ─────────────────────────────────────────────────────────────
# FILE 3: PIPELINE CONFIG
# ─────────────────────────────────────────────────────────────
def build_pipeline_config():
    """Central pipeline configuration read by all Phase 3–8 scripts."""
    return {
        "_meta": {
            "description": "Central pipeline configuration for Ameriprise Bank DE project",
            "created_at":  NOW,
            "version":     "1.0",
        },
        "aws": {
            "region":      AWS_REGION,
            "bucket":      BUCKET_NAME,
        },
        "zones": {
            "landing":    "landing/",
            "bronze":     "bronze/",
            "silver":     "silver/",
            "gold":       "gold/",
            "quarantine": "quarantine/",
            "metadata":   "metadata/",
        },
        "sources": {
            "rds": {
                "engine":   "sqlserver",
                "port":     1433,
                "database": "master",
                "schema":   "banking",
                "tables":   ["branches", "customers", "accounts", "transactions"],
            },
            "csv": {
                "payment_gateway": {
                    "landing_prefix": "landing/payment_gateway/",
                    "bronze_prefix":  "bronze/payment_gateway_logs/",
                    "file_pattern":   "payment_gateway_logs_*.csv",
                },
                "credit_bureau": {
                    "landing_prefix": "landing/credit_bureau/",
                    "bronze_prefix":  "bronze/credit_bureau_reports/",
                    "file_pattern":   "credit_bureau_reports_*.csv",
                }
            }
        },
        "dq": {
            "failure_threshold_pct": 5.0,        # Alert if >5% records fail DQ
            "quarantine_on_fail":    True,
            "halt_pipeline_on_critical": True,
        },
        "scheduling": {
            "incremental_cron": "30 20 * * ? *",  # 02:00 IST = 20:30 UTC
            "timezone":         "Asia/Kolkata",
            "max_runtime_hrs":  4,
        },
        "notifications": {
            "sns_topic_arn": "arn:aws:sns:ap-south-1:ACCOUNT_ID:banking-pipeline-alerts",
            "alert_on":      ["FAILURE", "DQ_BREACH", "SLA_BREACH"],
            "summary_on":    ["SUCCESS"],
        },
        "parquet": {
            "compression":    "snappy",
            "row_group_size": 100000,
        }
    }


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 2 Step 7: Create Metadata Files")
    print("=" * 65)

    s3 = boto3.client("s3", region_name=AWS_REGION)

    # ── 1. Watermark file ────────────────────────────────────────
    print(f"\n[1] Creating watermark.json ...")
    print(f"    Tracks last successful load per source.")
    print(f"    Phase 4 reads this to do incremental loads.")
    upload_json(s3, BUCKET_NAME,
                "metadata/watermarks/watermark.json",
                build_watermark(),
                "Pipeline watermark — last successful run per source")

    # ── 2. Data catalog ──────────────────────────────────────────
    print(f"\n[2] Creating data_catalog.json ...")
    print(f"    Documents every table and column in the data lake.")
    upload_json(s3, BUCKET_NAME,
                "metadata/catalog/data_catalog.json",
                build_data_catalog(),
                "Data catalog — table and column documentation")

    # ── 3. Pipeline config ───────────────────────────────────────
    print(f"\n[3] Creating pipeline_config.json ...")
    print(f"    Central config file — all future scripts read from here.")
    upload_json(s3, BUCKET_NAME,
                "metadata/pipeline_config.json",
                build_pipeline_config(),
                "Central pipeline configuration")

    # ── Verify ──────────────────────────────────────────────────
    print(f"\n[4] Verifying metadata files in S3 ...")
    expected_keys = [
        "metadata/watermarks/watermark.json",
        "metadata/catalog/data_catalog.json",
        "metadata/pipeline_config.json",
    ]
    for key in expected_keys:
        try:
            resp    = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            size_kb = resp["ContentLength"] / 1024
            print(f"    [OK]  s3://{BUCKET_NAME}/{key}  ({size_kb:.1f} KB)")
        except ClientError:
            print(f"    [FAIL] s3://{BUCKET_NAME}/{key}  — not found!")

    print(f"\n{'='*65}")
    print(f"  PHASE 2 COMPLETE!")
    print(f"  Your S3 data lake is fully set up:")
    print(f"")
    print(f"  s3://{BUCKET_NAME}/")
    print(f"    ├── landing/     (4 CSV files uploaded)")
    print(f"    ├── bronze/      (4 Parquet files + metadata cols)")
    print(f"    ├── silver/      (empty — Phase 5 will fill this)")
    print(f"    ├── gold/        (empty — Phase 6 will fill this)")
    print(f"    ├── quarantine/  (empty — DQ engine fills this)")
    print(f"    └── metadata/")
    print(f"          ├── watermark.json")
    print(f"          ├── data_catalog.json")
    print(f"          └── pipeline_config.json")
    print(f"")
    print(f"  WHAT'S NEXT (Phase 3):")
    print(f"  → Set up RDS SQL Server on AWS")
    print(f"  → Run 01_Create_Tables.sql, 02_Insert_Historical_data.sql")
    print(f"  → Run 03_Incrementat_data.sql")
    print(f"  → Test connection with Python pyodbc")
    print("=" * 65)


if __name__ == "__main__":
    main()
