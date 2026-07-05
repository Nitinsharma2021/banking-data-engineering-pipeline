PHASE 6 — COMPLETE IMPLEMENTATION GUIDE
Redshift Serverless + Athena + Apache Superset (Ubuntu)
Ameriprise Bank Data Engineering Project
================================================

QUICK REVISION
═══════════════
Phase 2 ✅  S3 Data Lake — bucket: neo-bank-datalake (ap-south-1)
Phase 3 ✅  RDS SQL Server — 4 banking tables
Phase 4 ✅  6 Silver Glue jobs done — DQ + PII masking
Phase 5 ✅  7 Gold Glue jobs done — star schema built
Phase 6 ⬅️  THIS PHASE — Redshift + Athena + Superset
Phase 7     Step Functions orchestration (next)
Phase 8     SNS alerting (next)


WHAT PHASE 6 BUILDS
════════════════════

  Gold Parquet (S3)
       ↓ COPY command
  Redshift Serverless tables
       ↓ SQL views
  Apache Superset dashboards (running on Ubuntu)
                OR
  AWS Athena (queries S3 directly, no loading needed)

11 REDSHIFT TABLES:
  banking.dim_date / dim_branch / dim_customer / dim_account
  banking.fact_transactions / fact_payments / fact_credit_risk
  banking.agg_daily_balances / agg_monthly_summary
  banking.agg_branch_performance / agg_customer_360

4 SQL VIEWS:
  banking.vw_branch_performance
  banking.vw_customer_risk_profile
  banking.vw_daily_txn_summary
  banking.vw_payment_channel_analysis


TOOLS USED IN PHASE 6
══════════════════════

TOOL 1: Amazon Redshift Serverless
  Why:  Cloud data warehouse, 10-100x faster than S3 for joins
  Cost: Free 3-month trial (300 RPU-hours), then ~$0.36/RPU-hour
        For this project: ~$2-3/month with careful use

TOOL 2: AWS Athena
  Why:  Queries S3 Parquet directly, zero loading needed
  Cost: $5 per TB scanned — your dataset ~$0.01 per query

TOOL 3: Apache Superset (FREE — runs on Ubuntu)
  Why:  Power BI Desktop is Windows only.
        Superset is the open source alternative, runs natively on Ubuntu.
        Connects to Redshift via SQLAlchemy.
  Cost: FREE (self-hosted)
  Install: Docker recommended (one command)

TOOL 4: Python scripts (psycopg2 + boto3)
  Automates Redshift table creation and data loading


EXECUTION ORDER
════════════════
  1.  AWS Console  → Create Redshift Serverless workgroup
  2.  AWS Console  → Open port 5439 in Security Group
  3.  AWS Console  → Verify IAM Role association
  4.  Terminal     → pip install psycopg2-binary
  5.  Terminal     → python step1_create_redshift_schema.py
  6.  Terminal     → python step2_load_gold_to_redshift.py
  7.  Terminal     → python step3_create_analytical_views.py
  8.  Terminal     → python step4_verify_redshift_layer.py
  9.  Terminal     → python step5_setup_athena.py
  10. Terminal     → Install Apache Superset (Docker)
  11. Superset UI  → Connect to Redshift + build 8 charts


PART A — CREATE REDSHIFT SERVERLESS (AWS CONSOLE)
═══════════════════════════════════════════════════

URL: https://console.aws.amazon.com/redshiftv2
Region: ap-south-1 (verify top right)

STEP 1 — Click "Create workgroup":
  Workgroup name:       neo-bank-workgroup
  Base capacity:        8 RPUs (minimum, cheapest)
  Enhanced VPC routing: OFF

STEP 2 — Create namespace:
  Namespace name:   neo-bank-namespace
  Admin username:   admin
  Admin password:   BankAdmin#2025
  Database name:    dev

STEP 3 — Permissions:
  Associate IAM role: AmerispriseBankGlueRole
  (Same role from Phase 4 — already has S3 access)

STEP 4 — Network:
  VPC:                default
  Publicly accessible: YES (CRITICAL — needed for laptop connection)

STEP 5 — Click Create
  Wait 5-10 minutes → Status: Available

STEP 6 — Copy your endpoint:
  Click neo-bank-workgroup → Workgroup details
  Endpoint format:
  neo-bank-workgroup.ACCOUNT_ID.ap-south-1.redshift-serverless.amazonaws.com
  Port: 5439
  PASTE this in REDSHIFT_HOST in all 4 Python scripts

neo-bank-workgroup.843302972838.ap-south-1.redshift-serverless.amazonaws.com:5439/dev


STEP 7 — Copy IAM Role ARN:
  IAM Console → Roles → AmerispriseBankGlueRole → copy ARN
  Format: arn:aws:iam::ACCOUNT_ID:role/AmerispriseBankGlueRole
  PASTE this in IAM_ROLE_ARN in step2 script

arn:aws:iam::843302972838:role/AmeripriseBankGlueRole

PART B — OPEN PORT 5439 IN SECURITY GROUP
═══════════════════════════════════════════

STEP 1 — Find your IP: https://whatismyip.com

STEP 2 — EC2 Console → Security Groups
  Find security group attached to Redshift workgroup

STEP 3 — Inbound rules → Edit → Add rule:
  Type:        Custom TCP
  Port:        5439
  Source:      My IP
  Description: Laptop Redshift access
  Save rules

INDIA ISP NOTE:
  Jio/Airtel IPs change frequently
  If connection fails next day → update rule with "My IP" again


PART C — VERIFY IAM ROLE ASSOCIATION
═════════════════════════════════════

Redshift Console → Serverless → Namespaces → neo-bank-namespace
→ Security and encryption tab
→ AmerispriseBankGlueRole must be listed

If not listed:
  Click "Manage IAM roles" → select role → Save


STEP 1 — INSTALL PYTHON DEPENDENCIES
══════════════════════════════════════
  source ~/Documents/Ameriprise_bank_project/myenv/bin/activate
  pip install psycopg2-binary==2.9.9
  pip install redshift-connector

Verify:
  python3 -c "import psycopg2; print('OK:', psycopg2.__version__)"


STEP 2 — CREATE SCHEMA + 11 TABLES
════════════════════════════════════
Script: step1_create_redshift_schema.py

BEFORE RUNNING: update REDSHIFT_HOST in script

RUN: python step1_create_redshift_schema.py

Creates schema dev.banking + 11 tables:
  dim_date, dim_branch, dim_customer, dim_account (4 dims)
  fact_transactions, fact_payments, fact_credit_risk (3 facts)
  agg_daily_balances, agg_monthly_summary,
  agg_branch_performance, agg_customer_360 (4 aggs)

EXPECTED: "ALL 11 TABLES CREATED SUCCESSFULLY"

Verify in Redshift Query Editor v2:
  SELECT table_name FROM information_schema.tables
  WHERE table_schema = 'banking'
  ORDER BY table_name;


STEP 3 — LOAD GOLD PARQUET INTO REDSHIFT
══════════════════════════════════════════
Script: step2_load_gold_to_redshift.py

BEFORE RUNNING — Update 2 values:
  REDSHIFT_HOST = "your-endpoint..."
  IAM_ROLE_ARN  = "arn:aws:iam::YOUR-ACCOUNT:role/AmerispriseBankGlueRole"

RUN: python step2_load_gold_to_redshift.py

For each table:
  [A] Checks Parquet files exist in s3://neo-bank-datalake/gold/...
  [B] Truncates existing data
  [C] COPY command loads Parquet from S3 (parallel, fast)
  [D] Verifies row count

EXPECTED:
  banking.dim_date              LOADED  3653 rows
  banking.dim_branch            LOADED     5 rows
  banking.dim_customer          LOADED   500+ rows
  banking.dim_account           LOADED  1000+ rows
  banking.fact_transactions     LOADED 30000+ rows
  banking.fact_payments         LOADED 20000+ rows
  banking.fact_credit_risk      LOADED  5500+ rows
  All agg_ tables               LOADED  varies


STEP 4 — CREATE 4 ANALYTICAL VIEWS
════════════════════════════════════
Script: step3_create_analytical_views.py

BEFORE RUNNING: update REDSHIFT_HOST

RUN: python step3_create_analytical_views.py

Creates 4 views — pre-joined for easy BI consumption:

  vw_branch_performance       → branch KPIs
  vw_customer_risk_profile    → customer + credit + history
  vw_daily_txn_summary        → daily breakdown by branch + channel
  vw_payment_channel_analysis → gateway success rate + processing time


STEP 5 — VERIFY COMPLETE LAYER
════════════════════════════════
Script: step4_verify_redshift_layer.py

BEFORE RUNNING: update REDSHIFT_HOST

RUN: python step4_verify_redshift_layer.py

Checks:
  [1] All 11 tables exist
  [2] Row counts match minimums
  [3] All 4 views exist
  [4] 4-table star schema JOIN works
  [5] 5 sample analytical queries run successfully


STEP 6 — SETUP ATHENA FOR DIRECT S3 QUERIES
══════════════════════════════════════════════
Script: step5_setup_athena.py

RUN: python step5_setup_athena.py

What it does:
  Creates Glue Crawler: neo-bank-gold-crawler
  S3 path: s3://neo-bank-datalake/gold/
  Registers gold_* tables in noe_bank_db

After running — use Athena Console:
  URL: https://console.aws.amazon.com/athena
  Database: noe_bank_db
  Run queries directly on gold S3 data:
    SELECT COUNT(*) FROM noe_bank_db.gold_fact_transactions;
    SELECT * FROM noe_bank_db.gold_agg_branch_performance;


STEP 7 — INSTALL APACHE SUPERSET ON UBUNTU (DOCKER)
══════════════════════════════════════════════════════

WHY SUPERSET FOR UBUNTU:
  Power BI Desktop is Windows-only
  Superset is the open source BI alternative — runs natively on Linux
  Connects to Redshift via SQLAlchemy
  Drag-and-drop charts like Power BI
  FREE (self-hosted)

PRE-REQUISITE: Install Docker on Ubuntu

  sudo apt-get update
  sudo apt-get install docker.io docker-compose-v2 -y
  sudo systemctl enable --now docker
  sudo usermod -aG docker $USER
  newgrp docker

  Verify: docker --version

INSTALL SUPERSET (one command):

  git clone https://github.com/apache/superset.git ~/superset
  cd ~/superset
  docker compose -f docker-compose-non-dev.yml pull
  docker compose -f docker-compose-non-dev.yml up -d

  Wait 5-10 minutes for first-time setup.

ACCESS SUPERSET:
  Browser: http://localhost:8088
  Default login:
    Username: admin
    Password: admin

INSTALL REDSHIFT DRIVER IN SUPERSET:

  docker compose -f docker-compose-non-dev.yml exec superset \
    pip install sqlalchemy-redshift redshift-connector

  docker compose -f docker-compose-non-dev.yml restart superset


STEP 8 — CONNECT SUPERSET TO REDSHIFT
═══════════════════════════════════════

In Superset UI (http://localhost:8088):

STEP 1 — Add database:
  Settings (top right) → Database Connections → + Database

STEP 2 — Choose Amazon Redshift

STEP 3 — Fill in SQLAlchemy URI:
  redshift+redshift_connector://admin:BankAdmin%23%402025@neo-bank-workgroup.ACCOUNT_ID.ap-south-1.redshift-serverless.amazonaws.com:5439/dev

  IMPORTANT — URL-encode the password special chars:
    # becomes %23
    @ becomes %40
    So BankAdmin#2025 → BankAdmin%232025

STEP 4 — Click "Test connection"
  Should show: Connection successful

STEP 5 — Connection name:
  Display Name: NeoBank Redshift
  Click Connect


STEP 9 — BUILD DASHBOARD IN SUPERSET
══════════════════════════════════════

In Superset:

STEP 1 — Register tables as datasets:
  Data → Datasets → + Dataset
  Select database: NeoBank Redshift
  Schema: banking
  Add these tables one by one:
    dim_branch, dim_customer, dim_date, dim_account
    fact_transactions, fact_credit_risk, fact_payments
    agg_branch_performance, agg_customer_360
    vw_branch_performance, vw_customer_risk_profile
    vw_daily_txn_summary, vw_payment_channel_analysis

STEP 2 — Create Dashboard:
  Dashboards → + Dashboard
  Name: Ameriprise Bank Analytics
  Save

STEP 3 — Build 8 charts:

CHART 1 — Branch Volume (Bar Chart):
  + Chart → Visualization: Bar Chart
  Dataset: vw_branch_performance
  X-axis: branch_name
  Metrics: SUM(total_volume_inr)
  Title: Total Transaction Volume by Branch

CHART 2 — Monthly Trend (Line Chart):
  Visualization: Line Chart
  Dataset: vw_daily_txn_summary
  X-axis: full_date
  Metrics: SUM(total_amount_inr)
  Title: Daily Transaction Trend

CHART 3 — KYC Pie:
  Visualization: Pie Chart
  Dataset: dim_customer
  Dimensions: kyc_status
  Metric: COUNT(*)
  Title: Customer KYC Distribution

CHART 4 — KPI: Total Transactions
  Visualization: Big Number
  Dataset: fact_transactions
  Metric: COUNT(*)
  Title: Total Transactions

CHART 5 — KPI: Total Volume INR
  Visualization: Big Number
  Dataset: fact_transactions
  Metric: SUM(amount)
  Title: Total Volume

CHART 6 — Risk Bands (Bar):
  Dataset: fact_credit_risk
  X-axis: risk_band
  Metric: COUNT(*)
  Title: Customer Credit Risk Distribution

CHART 7 — Gateway Performance (Table):
  Visualization: Table
  Dataset: vw_payment_channel_analysis
  Columns: gateway_name, total_payments, success_rate_pct
  Title: Payment Gateway Performance

CHART 8 — Weekend vs Weekday:
  Visualization: Bar Chart
  Custom SQL or join dim_date with fact_transactions
  X-axis: is_weekend
  Metric: SUM(amount)
  Title: Weekday vs Weekend Volume

STEP 4 — Add filters:
  Edit Dashboard → + Add Filter
  Filter 1: Branch (dim_branch.branch_name)
  Filter 2: Year (dim_date.year)


SAMPLE QUERIES — RUN IN REDSHIFT QUERY EDITOR
═══════════════════════════════════════════════

-- 1. Row counts for all tables
SELECT 'dim_branch' AS tbl, COUNT(*) AS rows FROM banking.dim_branch
UNION ALL SELECT 'dim_customer',     COUNT(*) FROM banking.dim_customer
UNION ALL SELECT 'fact_transactions', COUNT(*) FROM banking.fact_transactions
UNION ALL SELECT 'fact_payments',     COUNT(*) FROM banking.fact_payments
UNION ALL SELECT 'fact_credit_risk',  COUNT(*) FROM banking.fact_credit_risk
ORDER BY rows DESC;

-- 2. Branch performance
SELECT b.branch_name, b.city,
       COUNT(f.txn_id) AS txns,
       SUM(f.amount)   AS volume_inr
FROM banking.fact_transactions f
JOIN banking.dim_branch b ON f.branch_sk = b.branch_sk
GROUP BY b.branch_name, b.city
ORDER BY volume_inr DESC;

-- 3. Credit risk by band
SELECT risk_band,
       COUNT(*)       AS customers,
       AVG(credit_score) AS avg_score
FROM banking.fact_credit_risk
GROUP BY risk_band
ORDER BY avg_score DESC;

-- 4. Payment gateway success rate
SELECT gateway_name,
       COUNT(*) AS total,
       ROUND(SUM(is_success)*100.0/COUNT(*), 2) AS success_pct
FROM banking.fact_payments
GROUP BY gateway_name
ORDER BY total DESC;

-- 5. Top 10 customers by volume
SELECT c.customer_id, c.full_name,
       a.lifetime_txn_volume, a.lifetime_txn_count
FROM banking.agg_customer_360 a
JOIN banking.dim_customer c ON a.customer_sk = c.customer_sk
ORDER BY a.lifetime_txn_volume DESC LIMIT 10;


TROUBLESHOOTING
════════════════

ERROR: "could not connect to server: Connection refused"
FIX:
  1. Redshift Console → workgroup → Edit
     Enable "Publicly accessible"
  2. Security group port 5439 open for your current IP
  3. Your IP changed → update security group

ERROR: "password authentication failed for user admin"
FIX: Reset password: Redshift Console → namespace → Edit admin credentials

ERROR: "COPY: S3ServiceException Access Denied"
FIX:
  1. IAM role MUST be in namespace Security tab
  2. IAM role needs S3 read on neo-bank-datalake
  3. Verify IAM_ROLE_ARN in step2 script

ERROR: "relation banking.dim_branch does not exist"
FIX: Run step1_create_redshift_schema.py first

ERROR: Superset "Could not load database driver: Amazon Redshift"
FIX: Install driver inside container:
     docker compose -f docker-compose-non-dev.yml exec superset \
       pip install sqlalchemy-redshift redshift-connector
     Then restart container

ERROR: Superset cannot reach Redshift from container
FIX: In SQLAlchemy URI use the actual endpoint hostname,
     not localhost. Container has internet access.

ERROR: Docker permission denied
FIX: sudo usermod -aG docker $USER  (then logout/login)


PROMPT FOR PHASE 7 (paste in new chat)
═══════════════════════════════════════
I am building Ameriprise Bank AWS DE project. Completed:
  Phase 2 S3 Data Lake (neo-bank-datalake bucket, ap-south-1)
  Phase 3 RDS SQL Server with 4 banking tables
  Phase 4 6 Silver Glue Visual ETL jobs
  Phase 5 7 Gold Glue Visual ETL jobs — star schema
  Phase 6 Redshift Serverless 11 tables + 4 views loaded
          Apache Superset connected, dashboard built

Give me Phase 7: AWS Step Functions + EventBridge orchestration.
End-to-end same depth as previous phases. OS Ubuntu.
