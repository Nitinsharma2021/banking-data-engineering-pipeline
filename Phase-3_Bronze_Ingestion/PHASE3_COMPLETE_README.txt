PHASE 3 — COMPLETE GUIDE
AWS RDS SQL Server Setup for Ameriprise Bank DE Project
=======================================================

TABLE OF CONTENTS
  1. What Phase 3 builds
  2. Pre-requisites
  3. Step 1  — Create RDS SQL Server in AWS Console (MANUAL)
  4. Step 2  — Configure Security Group (MANUAL)
  5. Step 3  — Install DBeaver + Run 3 SQL scripts (MANUAL)
  6. Step 4  — Install ODBC Driver + Python packages
  7. Step 5  — Test RDS connection with Python
  8. Step 6  — Extract all 4 RDS tables → S3 Bronze
  9. Step 7  — Verify complete bronze layer
  10. Console verification checklist
  11. Troubleshooting all common errors
  12. What Phase 4 will do next

════════════════════════════════════════════════════════════════
1. WHAT PHASE 3 BUILDS
════════════════════════════════════════════════════════════════

After Phase 3, your pipeline has:

  SOURCE (AWS RDS SQL Server):
    banking.branches     →  5 rows  (BR001-BR005, including Chennai)
    banking.customers    →  500+ rows (KYC data, PAN numbers)
    banking.accounts     →  1000+ rows (Savings + Current accounts)
    banking.transactions →  30000+ rows (UPI/ATM/NEFT transactions)

  BRONZE ZONE (S3 — now complete with all 6 sources):
    bronze/branches/branches_2026-04-19.parquet          ← NEW (from RDS)
    bronze/customers/customers_2026-04-19.parquet        ← NEW (from RDS)
    bronze/accounts/accounts_2026-04-19.parquet          ← NEW (from RDS)
    bronze/transactions/transactions_2026-04-19.parquet  ← NEW (from RDS)
    bronze/payment_gateway_logs/...parquet               ← Phase 2 (CSV)
    bronze/credit_bureau_reports/...parquet              ← Phase 2 (CSV)

  METADATA (watermark updated for all 4 RDS sources):
    metadata/watermarks/watermark.json  ← last_run updated for RDS tables
    metadata/run_logs/phase3_run_*.json ← audit log for this extraction


════════════════════════════════════════════════════════════════
2. PRE-REQUISITES
════════════════════════════════════════════════════════════════

From Phase 2 (already done):
  [✓] S3 bucket ameriprise-bank-datalake exists
  [✓] All 6 bronze zone folders exist
  [✓] 2 CSV Parquet files in bronze/ (payment_gateway, credit_bureau)
  [✓] watermark.json, data_catalog.json, pipeline_config.json created
  [✓] Python, boto3, pandas, pyarrow installed in myenv

New for Phase 3:
  [ ] AWS RDS SQL Server instance created
  [ ] Microsoft ODBC Driver 18 installed on Ubuntu
  [ ] pyodbc Python package installed
  [ ] 3 SQL scripts run in DBeaver


════════════════════════════════════════════════════════════════
3. STEP 1 — CREATE RDS SQL SERVER (AWS CONSOLE — MANUAL STEP)
════════════════════════════════════════════════════════════════

Go to: https://console.aws.amazon.com/rds
Region: ap-south-1 (Mumbai) — verify top right

Click "Create database" and fill in EXACTLY:

  Creation method:     Standard create
  Engine:              Microsoft SQL Server
  Edition:             SQL Server Express Edition   ← FREE
  Version:             SQL Server 2019
  Template:            Free tier
  DB identifier:       ameriprise-bank-sqlserver
  Master username:     admin
  Master password:     BankAdmin#2025
  Instance class:      db.t3.micro
  Storage:             20 GB gp2, autoscaling OFF
  Public access:       YES   ← critical for laptop access
  VPC security group:  Create new → name: ameriprise-bank-rds-sg
  Auth:                Password authentication
  Enhanced monitoring: OFF (costs extra)

Click "Create database" → wait 10-15 minutes → status: Available

COPY YOUR ENDPOINT:
  Click your instance → Connectivity & security tab → copy Endpoint
  Example: ameriprise-bank-sqlserver.abc123def456.ap-south-1.rds.amazonaws.com
  Save this — every script needs it.
  
  example - ap-bank-sqlserver.cxis6seisyzq.ap-south-1.rds.amazonaws.com

COST:
  db.t3.micro SQL Server Express = FREE for 12 months (750 hrs/month)
  Storage 20 GB = FREE for 12 months
  After free tier: ~$0.020/hour = ~$14/month


════════════════════════════════════════════════════════════════
4. STEP 2 — CONFIGURE SECURITY GROUP (AWS CONSOLE — MANUAL)
════════════════════════════════════════════════════════════════

WHY: By default, no traffic can reach RDS. You must explicitly
     allow your laptop's IP on port 1433.

1. Find your IP: https://whatismyip.com  (note the number)
122.162.151.18
2. Go to: RDS Console → your instance → Connectivity & security
   → Click the security group: sg-xxxxxxx (ameriprise-bank-rds-sg)

3. Inbound rules tab → Edit inbound rules → Add rule:
   Type:       Custom TCP
   Port:       1433
   Source:     My IP   (auto-fills your IP)
   Description: My laptop SQL Server access

4. Save rules.

IMPORTANT NOTE FOR INDIA ISPs:
   Home broadband IPs (Jio, Airtel, BSNL) change frequently.
   If your connection drops tomorrow, your IP may have changed.
   Go back and update the security group with "My IP" again.

   BETTER ALTERNATIVE (if IP keeps changing):
   Source: 0.0.0.0/0 (allows ALL IPs)
   This is less secure but fine for a learning/dev project.
   Never do this in production.


════════════════════════════════════════════════════════════════
5. STEP 3 — INSTALL DBEAVER + RUN 3 SQL SCRIPTS
════════════════════════════════════════════════════════════════

INSTALL DBEAVER ON UBUNTU:
  sudo snap install dbeaver-ce
  (or download .deb from https://dbeaver.io/download/)

CONNECT DBEAVER TO RDS:
  New Connection → SQL Server
  Host:     your-endpoint.ap-south-1.rds.amazonaws.com
  Port:     1433
  Database: master
  User:     admin
  Password: BankAdmin#2025
  Test Connection → should say "Connected"
  (First time: DBeaver downloads JDBC drivers automatically)

RUN SCRIPTS IN ORDER:

Script 1 — 01_Create_Tables.sql:
  File → Open → select script → Ctrl+A → Ctrl+Enter
  Creates: banking schema + 4 tables (branches/customers/accounts/transactions)

Script 2 — 02_Insert_Historical_data.sql:
  File → Open → select script → Ctrl+A → Ctrl+Enter
  Inserts: 4 branches, 500 customers, 1000 accounts, 30000 transactions
  Takes: 1-2 minutes

Script 3 — 03_Incrementat_data.sql:
  File → Open → select script → Ctrl+A → Ctrl+Enter
  Adds: BR005 Chennai branch, ~500 new customers (MERGE strategy)
  Takes: 30-60 seconds

VERIFY IN DBEAVER (run these queries):
Summary of the Table:
Branches: Expanded from 4 to 5 (added Chennai Hub).

Customers: 4,000 original + 1,000 new records = 5,000 total.

Accounts: 4,500 original + 1,000 new records = 5,500 total.

Transactions: 15,000 historical + 5,000 new = 20,000 total.

════════════════════════════════════════════════════════════════
6. STEP 4 — INSTALL ODBC DRIVER + PYTHON PACKAGES
════════════════════════════════════════════════════════════════

Run ALL commands in Ubuntu terminal:

  # Add Microsoft repo
  curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
  curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list | \
      sudo tee /etc/apt/sources.list.d/mssql-release.list
  sudo apt-get update
  sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
  sudo apt-get install -y unixodbc-dev

  # Verify driver installed
  odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

  # Install Python packages (activate venv first)
  source ~/Documents/Ameriprise_bank_project/myenv/bin/activate
  pip install pyodbc==5.1.0 sqlalchemy==2.0.23

VERIFY:
  python3 -c "import pyodbc; print(pyodbc.drivers())"
  Should show: ['ODBC Driver 18 for SQL Server']


════════════════════════════════════════════════════════════════
7. STEP 5 — TEST RDS CONNECTION
════════════════════════════════════════════════════════════════

1. Open step3_test_rds_connection.py
2. Update RDS_ENDPOINT at the top with your actual endpoint
3. Run: python step3_test_rds_connection.py

Expected output:
  [PASS]  ODBC Driver 18 for SQL Server found
  [PASS]  TCP port 1433 reachable
  [PASS]  SQL Server authentication successful
  [PASS]  banking schema exists
  [PASS]  banking.branches
  [PASS]  banking.customers
  [PASS]  banking.accounts
  [PASS]  banking.transactions
  [PASS]  banking.branches: 5 rows
  [PASS]  banking.customers: 500+ rows
  [PASS]  banking.accounts: 1000+ rows
  [PASS]  banking.transactions: 30000+ rows
  [PASS]  BR005 Chennai Hub present


════════════════════════════════════════════════════════════════
8. STEP 6 — EXTRACT RDS TABLES TO S3 BRONZE
════════════════════════════════════════════════════════════════

NOte - Later Nanosecond timestamp will not working in glue to fix it first.


1. Open step4_extract_rds_to_s3.py
2. Update RDS_ENDPOINT at the top
3. Run: python step4_extract_rds_to_s3.py

What happens for each table:
  [A] Reads all rows from RDS SQL Server using pd.read_sql()
  [B] Converts datetime columns to ISO string format
  [C] Adds 9 metadata columns (src_system, batch_id, row_hash etc.)
  [D] Uploads directly to S3 as Parquet (in-memory, no temp files)
  [E] Verifies file exists in S3
  [F] Updates watermark.json with last_run timestamp

Expected output:
  [OK]  s3://ameriprise-bank-datalake/bronze/branches/branches_2026-04-19.parquet
  [OK]  s3://ameriprise-bank-datalake/bronze/customers/customers_2026-04-19.parquet
  [OK]  s3://ameriprise-bank-datalake/bronze/accounts/accounts_2026-04-19.parquet
  [OK]  s3://ameriprise-bank-datalake/bronze/transactions/transactions_2026-04-19.parquet


════════════════════════════════════════════════════════════════
9. STEP 7 — VERIFY COMPLETE BRONZE LAYER
════════════════════════════════════════════════════════════════

Run: python step5_verify_bronze_layer.py

This checks:
  - All 6 bronze/ prefixes have Parquet files
  - Reads each file from S3 and checks row counts
  - Verifies all 7 metadata columns present
  - Checks row_hash is valid MD5 format
  - Reads and displays watermark.json
  - Prints full bronze/ tree with sizes

Expected final output:
  [PASS]  ✓  branches
  [PASS]  ✓  customers
  [PASS]  ✓  accounts
  [PASS]  ✓  transactions
  [PASS]  ✓  payment_gateway_logs
  [PASS]  ✓  credit_bureau_reports
  PHASE 3 COMPLETE!


════════════════════════════════════════════════════════════════
10. CONSOLE VERIFICATION CHECKLIST (AWS CONSOLE)
════════════════════════════════════════════════════════════════

After completing all steps, verify in AWS Console:

RDS INSTANCE:
  → https://console.aws.amazon.com/rds
  → Your instance: ameriprise-bank-sqlserver
  → Status: Available  (green)
  → Engine: SQL Server Express 2019
  → Region: ap-south-1

S3 BRONZE ZONE:
  → S3 → ameriprise-bank-datalake → bronze/
  → Should see: branches/ customers/ accounts/ transactions/
                payment_gateway_logs/ credit_bureau_reports/
  → Click any folder → should see .parquet file
  → Click the .parquet file → Properties tab → Metadata section:
    data-zone = bronze, source-table = banking.customers etc.

WATERMARK UPDATED:
  → S3 → metadata/ → watermarks/ → watermark.json
  → Click → Open → browser shows JSON
  → banking.branches last_run_status = SUCCESS
  → banking.customers last_run_status = SUCCESS
  → banking.accounts last_run_status = SUCCESS
  → banking.transactions last_run_status = SUCCESS

RUN LOG:
  → S3 → metadata/ → run_logs/
  → Should see: phase3_run_2026-04-19_*.json
  → Click → Open → shows all 4 tables, row counts, batch_id


════════════════════════════════════════════════════════════════
11. TROUBLESHOOTING ALL COMMON ERRORS
════════════════════════════════════════════════════════════════

ERROR: "Communication link failure" or "TCP connection failed"
CAUSE: Security group not open / wrong IP
FIX:
  1. Find current IP: https://whatismyip.com
  2. EC2 Console → Security Groups → ameriprise-bank-rds-sg
  3. Edit inbound rules → update Source to "My IP"
  4. Also check: RDS instance has "Publicly accessible: Yes"
     RDS → your instance → Modify → Connectivity → Public access: Yes

ERROR: "Login failed for user 'admin'"
CAUSE: Wrong password in script
FIX:  Check RDS_PASSWORD in script matches what you set during RDS creation
      If forgot password: RDS Console → your instance → Modify → new password

ERROR: "Data source name not found" or "[IM002]"
CAUSE: ODBC Driver 18 not installed
FIX:  Run the curl/apt-get commands in step2 again
      Verify: odbcinst -q -d -n "ODBC Driver 18 for SQL Server"

ERROR: "Invalid object name 'banking.branches'"
CAUSE: 01_Create_Tables.sql not run yet, or ran with wrong database
FIX:  In DBeaver, make sure you are connected to "master" database
      Run 01_Create_Tables.sql again

ERROR: DBeaver "Connection refused"
CAUSE: Same as TCP failure above (security group)
FIX:  Update security group for your current IP

ERROR: "SSL connection error" in DBeaver
FIX:  In DBeaver connection properties → Driver Properties tab
      Add: TrustServerCertificate = true
           Encrypt = true

ERROR: pyodbc.OperationalError: timeout expired
CAUSE: Network too slow or RDS under load
FIX:  Increase timeout in connection string:
      f"Connection Timeout=60;"   (change 30 to 60)

ERROR: boto3 upload fails during step4
CAUSE: S3 permissions or bucket name wrong
FIX:  Verify: aws s3 ls s3://ameriprise-bank-datalake/bronze/
      If error: check IAM permissions for S3


════════════════════════════════════════════════════════════════
12. WHAT PHASE 4 WILL DO NEXT
════════════════════════════════════════════════════════════════

Phase 4 = AWS Glue Data Quality Engine

You will:
  1. Create AWS Glue Jobs (serverless Python scripts in the cloud)
  2. Read Parquet files from S3 bronze/ zone
  3. Run all 14 DQ checks:
     - Completeness: NULL checks on critical columns
     - Uniqueness: duplicate PK detection
     - Referential Integrity: FK validation across tables
     - Format: PAN regex, email format, phone length
     - Range: credit_score 300-900, balance >= 0, amount > 0
     - Business Rules: kyc_status values, account_type values
  4. Route PASS records → silver/ zone (cleaned Parquet)
  5. Route FAIL records → quarantine/ zone (with fail reason column)
  6. Write DQ results to metadata/dq_results/ (audit log)
  7. Trigger SNS email if failure rate > 5%

To start Phase 4, begin a fresh conversation and paste:
"I completed Phase 3 of the Ameriprise Bank AWS DE project.
RDS SQL Server has all 4 banking tables loaded.
Bronze zone has all 6 Parquet files (4 RDS + 2 CSV).
Give me Phase 4 - AWS Glue Data Quality Engine.
Bucket: ameriprise-bank-datalake, Region: ap-south-1, OS: Ubuntu"
