# PHASE 2 — COMPLETE GUIDE
# S3 Data Lake Setup for Ameriprise Bank DE Project
# ====================================================

## TABLE OF CONTENTS
  1. What you will build in Phase 2
  2. Pre-requisites (what you need before starting)
  3. Install all Python packages
  4. Understanding S3 concepts (for beginners)
  5. Step 1 — Verify AWS setup
  6. Step 2 — Create S3 bucket + folders
  7. Step 3 — Upload CSV files to landing zone
  8. Step 4 — Convert CSV to Parquet + add metadata columns
  9. Step 5 — Upload Parquet files to bronze zone
  10. Step 6 — Verify everything is correct
  11. Step 7 — Create metadata files (watermark, catalog, config)
  12. Troubleshooting common errors
  13. How to check your work in AWS Console (without code)
  14. What Phase 3 will do next


════════════════════════════════════════════════════════════════
## 1. WHAT YOU WILL BUILD IN PHASE 2
════════════════════════════════════════════════════════════════

By the end of Phase 2, your AWS S3 bucket will look like this:

  s3://ameriprise-bank-datalake/
    ├── landing/
    │     ├── payment_gateway/
    │     │     ├── payment_gateway_logs_1_20250115.csv      ← your original CSV
    │     │     └── payment_gateway_logs_2_incremental_20250115.csv
    │     └── credit_bureau/
    │           ├── credit_bureau_reports_1_20250115.csv
    │           └── credit_bureau_reports_2_incremental_20250115.csv
    │
    ├── bronze/
    │     ├── payment_gateway_logs/
    │     │     ├── payment_gateway_logs_historical_2025-01-15.parquet   ← 15,000 rows
    │     │     └── payment_gateway_logs_incremental_2025-01-15.parquet  ← 5,000 rows
    │     └── credit_bureau_reports/
    │           ├── credit_bureau_reports_historical_2025-01-15.parquet  ← 4,000 rows
    │           └── credit_bureau_reports_incremental_2025-01-15.parquet ← 1,500 rows
    │
    ├── silver/      ← EMPTY now (Phase 5 fills this)
    ├── gold/        ← EMPTY now (Phase 6 fills this)
    ├── quarantine/  ← EMPTY now (DQ engine fills this)
    └── metadata/
          ├── watermarks/watermark.json        ← "last time each source was loaded"
          ├── catalog/data_catalog.json        ← "what every column means"
          └── pipeline_config.json             ← central pipeline settings


════════════════════════════════════════════════════════════════
## 2. PRE-REQUISITES
════════════════════════════════════════════════════════════════

Before running any script, confirm these are done from Phase 1:

  [✓] AWS account created (aws.amazon.com)
  [✓] IAM user created (NOT root account)
  [✓] AWS CLI installed → test with: aws --version
  [✓] AWS CLI configured → test with: aws sts get-caller-identity
  [✓] Python 3.8+ installed → test with: python --version
  [✓] pip installed → test with: pip --version

Your 4 CSV files saved on your laptop:
  [✓] payment_gateway_logs_1.csv              (15,000 rows)
  [✓] payment_gateway_logs_2_incremental.csv  (5,000 rows)
  [✓] credit_bureau_reports_1.csv             (4,000 rows)
  [✓] credit_bureau_reports_2_incremental.csv (1,500 rows)

If AWS CLI is not configured:
  Run: aws configure
  Enter:
    AWS Access Key ID     → (from IAM user → Security credentials → Create access key)
    AWS Secret Access Key → (same screen)
    Default region name   → ap-south-1        ← Mumbai, closest to India
    Default output format → json


════════════════════════════════════════════════════════════════
## 3. INSTALL ALL PYTHON PACKAGES
════════════════════════════════════════════════════════════════

Open your terminal (Command Prompt on Windows, Terminal on Mac/Linux)

  cd path/to/phase2_guide/          # navigate to the folder with these scripts
  
  pip install -r requirements.txt

This installs:
  boto3        → AWS SDK for Python. You use this to talk to S3, Glue, etc.
  pandas       → Data manipulation library (read CSV, transform data)
  pyarrow      → Converts pandas DataFrames to Parquet format
  fastparquet  → Another Parquet engine (backup for pyarrow)
  python-dotenv → Loads environment variables from .env file
  tabulate     → Pretty-prints tables in terminal output

If you get "permission denied" errors on Mac/Linux:
  pip install -r requirements.txt --user

If you have multiple Python versions, use:
  pip3 install -r requirements.txt


════════════════════════════════════════════════════════════════
## 4. UNDERSTANDING S3 CONCEPTS (FOR BEGINNERS)
════════════════════════════════════════════════════════════════

S3 = Simple Storage Service. Think of it as a hard drive in the cloud.

KEY CONCEPTS:

  BUCKET:
    The top-level container. Like a hard drive name.
    Name: ameriprise-bank-datalake
    Must be globally unique across ALL AWS accounts worldwide.
    If "ameriprise-bank-datalake" is taken, use: ameriprise-bank-datalake-yourname-2025

  PREFIX (folder):
    S3 doesn't have real folders. It has "prefixes" — just part of the filename.
    "bronze/customers/file.parquet" is not a folder called "bronze".
    It's a file with the KEY: "bronze/customers/file.parquet"
    AWS Console shows these as folders for convenience.

  OBJECT:
    Any file you store in S3. Each object has:
    - Key (the file path e.g. "bronze/customers/customers_2025-01-15.parquet")
    - Value (the file content — bytes)
    - Metadata (tags about the file — we add project, batch_id, row_count etc.)
    - Version (if versioning is enabled)

  VERSIONING:
    S3 keeps every version of every file.
    You accidentally overwrote customers.parquet? No problem — go to S3 Console
    → Show Versions → restore the old one.
    Important for a banking project where audit trails matter.

  PARQUET:
    The best file format for data lakes. Why?
    - Columnar storage: if you query only 3 columns out of 20,
      it only reads those 3 columns (much faster than CSV which reads all)
    - Compressed: your 10 MB CSV becomes ~2 MB Parquet (Snappy compression)
    - Typed: stores column data types (int64, float64, datetime) — no guessing
    - Supported by: Athena, Glue, Redshift, Spark, Pandas — everything

  METADATA COLUMNS:
    Every file in bronze/ has these extra columns:
    - src_system:     "payment_gateway_csv" — which system it came from
    - src_file_name:  "payment_gateway_logs_1.csv" — original filename
    - load_timestamp: "2025-01-15T10:30:00Z" — when it was loaded
    - load_date:      "2025-01-15" — date only (for partitioning)
    - batch_id:       "a1b2c3d4-..." — UUID for this run (groups all files)
    - row_hash:       "ab12cd34..." — MD5 fingerprint of source data
    - is_active:      1 — marks record as current (0 = soft deleted)
    - pipeline_phase: "bronze" — which zone this belongs to

  LIFECYCLE RULES:
    Auto-delete or archive files after X days (saves money).
    We set:
    - landing/ → auto-delete after 30 days (CSV converted to Parquet)
    - quarantine/ → move to cheaper STANDARD_IA storage after 90 days
    - bronze/ → archive to Glacier (cheapest) after 365 days


════════════════════════════════════════════════════════════════
## 5. STEP 1 — VERIFY AWS SETUP
════════════════════════════════════════════════════════════════

Run:   python step1_verify_aws_setup.py

WHAT IT DOES:
  - Checks your AWS credentials are valid
  - Prints your Account ID and IAM user ARN
  - Confirms your region is set
  - Tests S3 list permissions
  - Checks all required Python packages are installed

EXPECTED OUTPUT (when everything is working):
  ============================================================
    ALL CHECKS PASSED — Ready to run Step 2!
  ============================================================

COMMON ERRORS AND FIXES:
  
  Error: "NoCredentialsError: Unable to locate credentials"
  Fix:   Run: aws configure
         Enter your Access Key ID and Secret Access Key

  Error: "ClientError: An error occurred (AuthFailure)"
  Fix:   Your Access Key is wrong. Go to:
         AWS Console → IAM → Users → Your User → Security Credentials
         → Create new Access Key → copy both values → aws configure

  Error: "ModuleNotFoundError: No module named 'boto3'"
  Fix:   Run: pip install boto3


════════════════════════════════════════════════════════════════
## 6. STEP 2 — CREATE S3 BUCKET + ALL ZONE FOLDERS
════════════════════════════════════════════════════════════════

BEFORE RUNNING — Edit step2_create_s3_bucket.py:
  Change this at the top of the file:
    AWS_REGION  = "ap-south-1"                   ← keep this (Mumbai)
    BUCKET_NAME = "ameriprise-bank-datalake"     ← change if name is taken

  If "ameriprise-bank-datalake" is already taken (error: BucketAlreadyExists):
    Change to: "ameriprise-bank-datalake-yourname-2025"
    IMPORTANT: Update BUCKET_NAME in EVERY script to match.

Run:   python step2_create_s3_bucket.py

WHAT IT DOES:
  [1] Creates S3 bucket
  [2] Enables versioning (keeps all file history)
  [3] Enables AES256 encryption (all data protected at rest — free)
  [4] Blocks public access (banking data must be private)
  [5] Creates 34 zone folders (landing/bronze/silver/gold/quarantine/metadata)
  [6] Adds lifecycle rules (auto-archive/delete old files)
  [7] Adds resource tags (for cost tracking)

EXPECTED OUTPUT:
  [CREATED]  s3://ameriprise-bank-datalake
  [ENABLED]  Versioning ON
  [ENABLED]  AES256 encryption ON
  [BLOCKED]  Public access: OFF
  [CREATED]  s3://ameriprise-bank-datalake/landing/payment_gateway/
  [CREATED]  s3://ameriprise-bank-datalake/bronze/customers/
  ... (34 folders total)
  
  
  

VERIFY IN AWS CONSOLE:
  1. Go to: https://s3.console.aws.amazon.com
  2. Click your bucket: ameriprise-bank-datalake
  3. You should see: landing/ bronze/ silver/ gold/ quarantine/ metadata/
  4. Click into bronze/ → you should see: branches/ customers/ accounts/ etc.

To Verify 
(myenv) (base) shreyansh-jain@shreyansh-jain-HP-ProBook-6470b:~/Documents/Ameriprise_bank_project$ aws s3api get-bucket-versioning \
  --bucket ameriprise-bank-datalake
{
    "Status": "Enabled"
}
(myenv) (base) shreyansh-jain@shreyansh-jain-HP-ProBook-6470b:~/Documents/Ameriprise_bank_project$ aws s3api get-bucket-encryption \
  --bucket ameriprise-bank-datalake
{
    "ServerSideEncryptionConfiguration": {
        "Rules": [
            {
                "ApplyServerSideEncryptionByDefault": {
                    "SSEAlgorithm": "AES256"
                },
                "BucketKeyEnabled": false
            }
        ]
    }
}
(myenv) (base) shreyansh-jain@shreyansh-jain-HP-ProBook-6470b:~/Documents/Ameriprise_bank_project$ aws s3api get-public-access-block \
  --bucket ameriprise-bank-datalake
{
    "PublicAccessBlockConfiguration": {
        "BlockPublicAcls": true,
        "IgnorePublicAcls": true,
        "BlockPublicPolicy": true,
        "RestrictPublicBuckets": true
    }
}
(myenv) (base) shreyansh-jain@shreyansh-jain-HP-ProBook-6470b:~/Documents/Ameriprise_bank_project$ 





════════════════════════════════════════════════════════════════
## 7. STEP 3 — UPLOAD CSV FILES TO S3 LANDING ZONE
════════════════════════════════════════════════════════════════

BEFORE RUNNING — Edit step3_upload_csv_files.py:
  Change LOCAL_CSV_FOLDER to the folder where your 4 CSV files are:
  
  Windows: LOCAL_CSV_FOLDER = "/home/shreyansh-jain/Documents/Ameriprise_bank_project/phase2_complete_package/phase2_guide/Blob_Vendor_Data"
  Mac:     LOCAL_CSV_FOLDER = "/home/shreyansh-jain/Documents/Ameriprise_bank_project/phase2_complete_package/phase2_guide/Blob_Vendor_Data"
  
  If the CSV files are in the same folder as these scripts: LOCAL_CSV_FOLDER = "."

Run:   python step3_upload_csv_files.py

WHAT IT DOES:
  - Uploads your 4 CSV files to S3 landing/ zone
  - Adds upload date to filename: payment_gateway_logs_1_20250115.csv
  - Adds S3 object metadata (project, source, load_type)
  - Shows upload progress bar
  - Verifies each file exists in S3 after upload

THE 4 FILES AND WHERE THEY GO:
  payment_gateway_logs_1.csv              → landing/payment_gateway/
  payment_gateway_logs_2_incremental.csv  → landing/payment_gateway/
  credit_bureau_reports_1.csv             → landing/credit_bureau/
  credit_bureau_reports_2_incremental.csv → landing/credit_bureau/

EXPECTED OUTPUT:
  File   : payment_gateway_logs_1.csv
  Desc   : Payment Gateway Logs — Historical
  Size   : 0.82 MB
  S3 Key : s3://ameriprise-bank-datalake/landing/payment_gateway/payment_gateway_logs_1_20250115.csv
  Upload : [████████████████████] 100%
  [OK]    Verified in S3 — 820,480 bytes


════════════════════════════════════════════════════════════════
## 8. STEP 4 — CONVERT CSV TO PARQUET + ADD METADATA COLUMNS
════════════════════════════════════════════════════════════════



TO Verify file will be creete in your Local.

So why we uploaded the payment log and creadit log file into s3 in previous step12:24 PMGreat question! Here's the clear difference between what each step did:

Step 3 — Uploaded RAW CSV files to landing/
landing/payment_gateway/payment_gateway_logs_1_20260419.csv     ← original CSV
landing/credit_bureau/credit_bureau_reports_1_20260419.csv      ← original CSV
Think of landing/ as your inbox — exactly like how a vendor drops a file in your email. No processing done. Raw file as-is.

Step 5 — Uploads PROCESSED Parquet files to bronze/
bronze/payment_gateway_logs/payment_gateway_logs_historical_2026-04-19.parquet   ← converted + metadata added
bronze/credit_bureau_reports/credit_bureau_reports_historical_2026-04-19.parquet ← converted + metadata added
Bronze is your first processed layer — same data but now in Parquet format with 8 extra metadata columns added.

Why keep both?
            landing/ 						(CSV)bronze/(Parquet)
Format		Raw  				         	 CSVParquet + metadata
Purpose     Proof the original file arrived	 Pipeline's working copy
Who uses itAudit / reprocessingAll downstream pipeline stepsAuto-deleted?Yes, after 30 daysNo, kept forever

Real world analogy:
landing/  =  Vendor emails you a raw Excel file
bronze/   =  You open it, clean the format, save it properly in your system
The landing/ copy answers "what exactly did the vendor send us?"
The bronze/ copy answers "what did our pipeline receive and process?"
If bronze data ever looks wrong, you go back to landing/ and reprocess from scratch. That's why both exist.



Run:   python step4_csv_to_parquet.py

WHAT IT DOES (for each CSV file):
  [A] Read CSV file with pandas
  [B] Enforce correct data types (int64, float64, string, datetime)
  [C] Add 8 metadata columns (src_system, batch_id, row_hash etc.)
  [D] Print stats (row count, null counts, memory usage)
  [E] Save as Parquet with Snappy compression
  [F] Verify Parquet file is readable (read-back check)

FILES CREATED (in local output_parquet/ folder):
  output_parquet/
    bronze/payment_gateway_logs/
      payment_gateway_logs_historical_2025-01-15.parquet
      payment_gateway_logs_incremental_2025-01-15.parquet
    bronze/credit_bureau_reports/
      credit_bureau_reports_historical_2025-01-15.parquet
      credit_bureau_reports_incremental_2025-01-15.parquet
    batch_manifest_2025-01-15.json

UNDERSTANDING METADATA COLUMNS:
  Every row in every Parquet file has these extra columns:

  src_system     = "payment_gateway_csv"
    → Tells you: this row came from a payment gateway CSV file
    → In Phase 4 when you add SQL Server data, it will be "banking_rds"

  src_file_name  = "payment_gateway_logs_1.csv"
    → Traceability: if data is wrong, you know exactly which file it came from

  load_timestamp = "2025-01-15T10:30:45.123Z"
    → Audit trail: when this row was loaded into the pipeline

  load_date      = "2025-01-15"
    → Date-only version (used for partitioning Parquet files by date)

  batch_id       = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    → UUID (universally unique ID). All 4 files in this run share the SAME batch_id
    → Lets you say "show me everything loaded in batch abc123"

  row_hash       = "d41d8cd98f00b204e9800998ecf8427e"
    → MD5 fingerprint of ALL source column values in this row
    → In Silver layer, if row_hash changes → that row was updated → apply SCD2

  is_active      = 1
    → 1 = this is the current version of this record
    → 0 = this record was soft-deleted (we never hard-delete in a data lake)

  pipeline_phase = "bronze"
    → Reminder of which zone this file belongs to

WHAT IS A BATCH MANIFEST?
  batch_manifest_2025-01-15.json is a JSON file that tracks everything in this batch:
  {
    "batch_id": "a1b2c3d4-...",
    "run_date": "2025-01-15",
    "files": [
      {"file": "payment_gateway_logs_1.csv", "status": "CONVERTED", "rows": 15000},
      ...
    ]
  }
  Useful later when you want to know "what did run on Jan 15 contain?"


════════════════════════════════════════════════════════════════
## 9. STEP 5 — UPLOAD PARQUET FILES TO S3 BRONZE ZONE
════════════════════════════════════════════════════════════════

Run:   python step5_upload_parquet_to_s3.py

WHAT IT DOES:
  - Finds all .parquet files in output_parquet/ folder
  - Reads their row count and schema before uploading
  - Uploads each to the correct S3 bronze/ prefix
  - Adds S3 object metadata (row-count, compression, data-zone)
  - Verifies each file in S3 after upload
  - Uploads the batch manifest to metadata/run_logs/

S3 KEY PATTERN:
  Local path:  output_parquet/bronze/payment_gateway_logs/payment_gateway_logs_historical_2025-01-15.parquet
  S3 key:      bronze/payment_gateway_logs/payment_gateway_logs_historical_2025-01-15.parquet

IMPORTANT — WHY WE KEEP OLD FILES:
  For incremental loads, new files are ADDED, not replaced.
  After the next run:
    bronze/payment_gateway_logs/payment_gateway_logs_historical_2025-01-15.parquet  ← original
    bronze/payment_gateway_logs/payment_gateway_logs_incremental_2025-02-01.parquet ← new
  
  Both files stay. Bronze is your permanent audit history.
  You can always query "show me all transactions from Jan 15" even months later.


════════════════════════════════════════════════════════════════
## 10. STEP 6 — VERIFY EVERYTHING IS CORRECT
════════════════════════════════════════════════════════════════

Run:   python step6_verify_s3_structure.py

WHAT IT CHECKS:
  [1] Bucket settings: versioning, encryption, public access, lifecycle rules
  [2] All 34 expected folders present
  [3] Bronze Parquet files uploaded (lists them with sizes)
  [4] Parquet content: reads file from S3, checks all metadata columns,
      verifies row hash is MD5 format, checks no nulls in critical columns
  [5] Prints full S3 tree with file sizes

EXPECTED FINAL OUTPUT:
  ============================================================
    VERIFICATION REPORT
  ============================================================
  [PASS]  ✓  Bucket settings
  [PASS]  ✓  Zone folder structure
  [PASS]  ✓  Bronze Parquet files
  [PASS]  ✓  Parquet content

    ALL CHECKS PASSED
    Your S3 Bronze layer is correctly set up!
  ============================================================


════════════════════════════════════════════════════════════════
## 11. STEP 7 — CREATE METADATA FILES
════════════════════════════════════════════════════════════════

Run:   python step7_create_metadata_files.py

WHAT IT CREATES:

  [1] metadata/watermarks/watermark.json
      Contains last_successful_run timestamp for each source:
      {
        "sources": {
          "banking.customers": {
            "last_successful_run": "1900-01-01T00:00:00Z",   ← set to epoch = never run yet
            "last_run_status": "NEVER_RUN",
            "watermark_column": "updated_at"
          },
          "payment_gateway_csv": {...},
          ...
        }
      }
      In Phase 4, after each successful run, the pipeline updates these timestamps.
      Next run reads them: "only pull rows where updated_at > 2025-01-15T02:00:00Z"

  [2] metadata/catalog/data_catalog.json
      Documents EVERY column in EVERY table:
      {
        "bronze.payment_gateway_logs": {
          "columns": {
            "txn_id":         {"type":"int64","pii":false,"pk":true,"description":"..."},
            "gateway_name":   {"type":"string","valid_values":["Stripe","BillDesk",...],...},
            "pan_number":     {"type":"string","pii":true,"classification":"PII-CONFIDENTIAL",...}
          }
        }
      }
      This is your data dictionary. New team member joins? Point them to this file.

  [3] metadata/pipeline_config.json
      Central configuration:
      {
        "aws":   {"region":"ap-south-1","bucket":"ameriprise-bank-datalake"},
        "dq":    {"failure_threshold_pct":5.0},
        "scheduling": {"incremental_cron":"30 20 * * ? *"},
        "notifications": {"sns_topic_arn":"arn:aws:sns:..."}
      }
      All Phase 4–8 scripts read this file instead of having hardcoded values.


════════════════════════════════════════════════════════════════
## 12. TROUBLESHOOTING COMMON ERRORS
════════════════════════════════════════════════════════════════

ERROR: BucketAlreadyExists
  CAUSE:  S3 bucket names are globally unique. Someone else already has that name.
  FIX:    Change BUCKET_NAME to something unique:
            "ameriprise-bank-datalake-raj-2025"
          Update BUCKET_NAME in ALL 7 scripts.

ERROR: NoCredentialsError
  CAUSE:  AWS CLI not configured with credentials.
  FIX:    Run: aws configure
          Then run: aws sts get-caller-identity
          Should show your Account ID.

ERROR: ClientError: Access Denied
  CAUSE:  Your IAM user doesn't have S3 permissions.
  FIX:    Go to: IAM → Users → Your User → Permissions
          → Add permissions → Attach policy → AmazonS3FullAccess
          OR attach AdministratorAccess (easier for dev)

ERROR: FileNotFoundError: payment_gateway_logs_1.csv
  CAUSE:  Script can't find your CSV file.
  FIX:    In step3 and step4, update LOCAL_CSV_FOLDER to the full path:
          Windows: LOCAL_CSV_FOLDER = r"C:\Users\YourName\Downloads"
          Mac:     LOCAL_CSV_FOLDER = "/Users/yourname/Downloads"

ERROR: ModuleNotFoundError: No module named 'pyarrow'
  FIX:    pip install pyarrow fastparquet

ERROR: InvalidLocationConstraint
  CAUSE:  You're using us-east-1 but passing LocationConstraint.
  FIX:    In step2, the code already handles this. If you see this error,
          check your AWS_REGION is not "us-east-1" OR
          ensure you're running the step2 script provided (it has the fix).

ERROR: Parquet file won't open / read_parquet fails
  CAUSE:  Corrupted download or wrong version of pyarrow.
  FIX:    pip install --upgrade pyarrow
          Then re-run step4.


════════════════════════════════════════════════════════════════
## 13. HOW TO CHECK YOUR WORK IN AWS CONSOLE (WITHOUT CODE)
════════════════════════════════════════════════════════════════

After completing all 7 steps, verify everything visually in AWS Console:

S3 BUCKET:
  1. Go to https://console.aws.amazon.com/s3
  2. Click "ameriprise-bank-datalake"
  3. You should see 6 zone folders: landing/ bronze/ silver/ gold/ quarantine/ metadata/

CHECK BRONZE PARQUET FILES:
  4. Click bronze/ → payment_gateway_logs/
  5. You should see: payment_gateway_logs_historical_2025-XX-XX.parquet
  6. Click the file → Object overview → Metadata tab
  7. You should see: data-zone=bronze, row-count=15000 etc.

CHECK VERSIONING:
  8. Go to bucket → Properties tab
  9. Scroll to "Bucket Versioning" → should say "Enabled"

CHECK ENCRYPTION:
  10. Same Properties tab → "Default encryption" → should say "AES-256"

CHECK PUBLIC ACCESS:
  11. Permissions tab → Block Public Access → all 4 should be "On"

CHECK LIFECYCLE RULES:
  12. Management tab → Lifecycle rules → should see 3 rules

VIEW WATERMARK FILE:
  13. Go to metadata/ → watermarks/ → watermark.json
  14. Click → Open button (top right) → opens in browser tab as JSON


════════════════════════════════════════════════════════════════
## 14. WHAT PHASE 3 WILL DO NEXT
════════════════════════════════════════════════════════════════

Phase 3 = Set up AWS RDS SQL Server (your source database)

You will:
  1. Create an RDS SQL Server instance (db.t3.micro — free tier)
  2. Configure VPC security group (allow your IP on port 1433)
  3. Connect to RDS using DBeaver or Azure Data Studio (free tools)
  4. Run 01_Create_Tables.sql → creates banking schema with 4 tables
  5. Run 02_Insert_Historical_data.sql → loads 500+ customers, accounts, transactions
  6. Run 03_Incrementat_data.sql → adds Chennai branch + incremental records
  7. Write a Python test script that reads from RDS using pyodbc
  8. Confirm: SELECT * FROM banking.branches → shows 5 rows (BR001-BR005)

After Phase 3, you will have:
  ✓ S3 Data Lake (Phase 2 — done now)
  ✓ RDS SQL Server with banking data (Phase 3)

Then Phase 4 builds the actual ingestion pipeline that reads from RDS
and writes to your S3 Bronze zone as Parquet files — completing the loop!
