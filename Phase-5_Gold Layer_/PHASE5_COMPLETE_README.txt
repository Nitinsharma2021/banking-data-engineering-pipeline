
PHASE 5 — COMPLETE GUIDE
AWS Glue Visual ETL — Gold Layer (Star Schema)
Ameriprise Bank Data Engineering Project
================================================

════════════════════════════════════════════════
QUICK REVISION: WHAT EACH PHASE DID
════════════════════════════════════════════════

Phase 2 ✅  S3 Data Lake created
           → bronze/ silver/ gold/ quarantine/ metadata/ zones
           → 4 CSV files converted to Parquet (payment_gateway, credit_bureau)
           → watermark.json + data_catalog.json + pipeline_config.json created

Phase 3 ✅  RDS SQL Server set up on AWS
           → 4 banking tables created (branches/customers/accounts/transactions)
           → Historical + incremental SQL data loaded
           → All 4 RDS tables extracted to S3 bronze/ as Parquet with 9 metadata cols

Phase 4 ✅  AWS Glue Visual ETL — Data Quality Engine
           → 6 Glue jobs created (one per source table)
           → 14 DQ rules across 6 check types (Completeness/Uniqueness/FK/Format/Range/Business)
           → PASS records → silver/ (PII masked, standardized, enriched)
           → FAIL records → quarantine/ (with fail reason)
           → Silver layer now has 6 clean Parquet tables

Phase 5 ⬅️  THIS PHASE — Gold Layer Star Schema
           → Read silver/ tables → build analytics-ready star schema
           → 4 Dimension tables + 3 Fact tables + 4 Aggregation tables
           → All written to gold/ zone in S3 as Parquet


════════════════════════════════════════════════
TABLE OF CONTENTS — PHASE 5
════════════════════════════════════════════════

  1.  What Phase 5 builds (star schema design)
  2.  Pre-requisites
  3.  Script: step1_create_dim_date.py  (run locally — no Glue needed)
  4.  PART A  — Glue Visual ETL Job 1: silver_to_gold_dim_branch
  5.  PART B  — Glue Visual ETL Job 2: silver_to_gold_dim_customer
  6.  PART C  — Glue Visual ETL Job 3: silver_to_gold_dim_account
  7.  PART D  — Glue Visual ETL Job 4: silver_to_gold_fact_transactions
  8.  PART E  — Glue Visual ETL Job 5: silver_to_gold_fact_payments
  9.  PART F  — Glue Visual ETL Job 6: silver_to_gold_fact_credit_risk
  10. PART G  — Glue Visual ETL Job 7: silver_to_gold_aggregations
  11. Script: step2_run_gold_jobs.py
  12. Script: step3_verify_gold_layer.py
  13. Console verification checklist
  14. Troubleshooting
  15. Prompt to start Phase 6


════════════════════════════════════════════════
1. WHAT PHASE 5 BUILDS — STAR SCHEMA DESIGN
════════════════════════════════════════════════

STAR SCHEMA (read from top to bottom):

                   ┌──────────────────┐
                   │  dim_date        │
                   │  date_sk (PK)    │
                   │  full_date       │
                   │  day/month/year  │
                   │  quarter/week    │
                   │  is_weekend      │
                   └────────┬─────────┘
                            │
  ┌──────────────┐    ┌─────▼──────────────┐    ┌──────────────────┐
  │  dim_branch  │    │  fact_transactions │    │  dim_account     │
  │  branch_sk   ├────│  txn_sk (PK)       ├────│  account_sk (PK) │
  │  branch_code │    │  account_sk (FK)   │    │  account_id      │
  │  branch_name │    │  customer_sk (FK)  │    │  account_type    │
  │  city/state  │    │  branch_sk (FK)    │    │  balance         │
  │  region      │    │  date_sk (FK)      │    │  status          │
  └──────────────┘    │  txn_type          │    │  customer_sk(FK) │
                      │  amount            │    └──────────────────┘
  ┌──────────────┐    │  channel           │
  │  dim_customer│    │  status            │
  │  customer_sk ├────└────────────────────┘
  │  customer_id │
  │  first_name  │    ┌────────────────────┐
  │  last_name   │    │  fact_payments     │
  │  kyc_status  ├────│  payment_sk (PK)   │
  │  branch_sk   │    │  txn_id            │
  │  pan_masked  │    │  gateway_name      │
  │  email_masked│    │  gateway_status    │
  └──────────────┘    │  processing_time   │
                      │  device_type       │
                      │  geo_location      │
                      │  date_sk (FK)      │
                      └────────────────────┘

                      ┌────────────────────┐
                      │  fact_credit_risk  │
                      │  credit_sk (PK)    │
                      │  customer_sk (FK)  │
                      │  date_sk (FK)      │
                      │  credit_score      │
                      │  risk_grade        │
                      │  risk_band         │
                      │  active_loans      │
                      │  overdue_amount    │
                      └────────────────────┘

AGGREGATION TABLES (pre-computed for fast BI):
  gold/agg_daily_balances/      → daily balance per account
  gold/agg_monthly_summary/     → monthly txn totals per branch
  gold/agg_branch_performance/  → branch KPIs (txn count, volume, avg)
  gold/agg_customer_360/        → full customer profile view

GOLD GLUE JOBS (7 total):
  Job 1: silver_to_gold_dim_branch       (reads: silver/branches/)
  Job 2: silver_to_gold_dim_customer     (reads: silver/customers/)
  Job 3: silver_to_gold_dim_account      (reads: silver/accounts/ + silver/customers/)
  Job 4: silver_to_gold_fact_transactions(reads: silver/transactions/ + dims)
  Job 5: silver_to_gold_fact_payments    (reads: silver/payment_gateway_logs/)
  Job 6: silver_to_gold_fact_credit_risk (reads: silver/credit_bureau_reports/)
  Job 7: silver_to_gold_aggregations     (reads: gold/fact_transactions/)

RUN ORDER (must follow this sequence — dimensions before facts):
  1 → dim_branch
  2 → dim_customer
  3 → dim_account     (needs dim_customer)
  4 → fact_transactions (needs all dims)
  5 → fact_payments
  6 → fact_credit_risk
  7 → aggregations    (needs fact_transactions)


════════════════════════════════════════════════
2. PRE-REQUISITES
════════════════════════════════════════════════

From Phase 4 (must be complete):
  [✓] All 6 silver/ Parquet tables exist in S3
  [✓] AmerispriseBankGlueRole IAM role exists
  [✓] neo_bank_db Glue database exists
  [✓] silver/ tables registered in Glue Data Catalog

One-time setup before creating Glue jobs:
  → Run: python step1_create_dim_date.py
     This creates dim_date table (date dimension for 2020-2030)
     and uploads it to s3://ameriprise-bank-datalake/gold/dim_date/

  → Run Glue Crawler for Silver zone:
     Glue Console → Crawlers → Create crawler
     Name:       ameriprise-silver-crawler
     Data source: s3://ameriprise-bank-datalake/silver/
     IAM Role:   AmerispriseBankGlueRole
     Database:   ameriprise_bank_db
     Prefix:     silver_
     Run crawler → registers all 6 silver tables in catalog


════════════════════════════════════════════════
PART A — GLUE VISUAL ETL JOB 1: silver_to_gold_dim_branch
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/branches/ → adds surrogate key (branch_sk) → writes gold/dim_branch/
  Simple job — branches table has only 5 rows, no joins needed.

─────────────────────────────────────────────────
A1. JOB DETAILS (Job Details tab — top right)
─────────────────────────────────────────────────
  Name:              silver_to_gold_dim_branch
  IAM Role:          AmerispriseBankGlueRole
  Glue version:      Glue 4.0
  Language:          Python 3
  Worker type:       G.1X
  Workers:           2
  Timeout (mins):    30
  Max retries:       1

Job parameters (click "+ Add new parameter" for each):
  Key: --silver_branches_path    Value: s3://ameriprise-bank-datalake/silver/branches/
  Key: --gold_dim_branch_path    Value: s3://ameriprise-bank-datalake/gold/dim_branch/
  Key: --table_name              Value: dim_branch

─────────────────────────────────────────────────
A2. SOURCE NODE
─────────────────────────────────────────────────
→ Canvas → "+" → Source → Amazon S3

  Node name:     Source_Silver_Branches
  S3 URL:        s3://ameriprise-bank-datalake/silver/branches/
  Format:        Parquet
  Recursive:     ✓ checked
  Infer Schema:  ✓ → click "Infer Schema" button

Columns you will see:
  branch_code (string), branch_name (string), city (string),
  state (string), region (string), created_at (string),
  src_system (string), batch_id (string), row_hash (string),
  load_date (string), is_active (int), silver_load_ts (string),
  silver_layer (string), dq_status (string)

─────────────────────────────────────────────────
A3. CUSTOM TRANSFORM NODE — Build dim_branch
─────────────────────────────────────────────────
→ "+" → Transform → Custom transform

  Node name:    Transform_dim_branch
  Data source:  Source_Silver_Branches

Paste this code EXACTLY:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import (
        col, lit, row_number, monotonically_increasing_id,
        current_timestamp, trim, upper, when
    )
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 1. Add surrogate key (branch_sk)
    #    Use row_number for deterministic ordering
    window = Window.orderBy("branch_code")
    df = df.withColumn("branch_sk", row_number().over(window))

    # 2. Select and rename only the columns needed for Gold dim
    df = df.select(
        col("branch_sk"),
        col("branch_code"),
        col("branch_name"),
        col("city"),
        col("state"),
        col("region"),
        col("created_at").alias("source_created_at"),
    )

    # 3. Add Gold metadata columns
    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts",  lit(now_ts))
    df = df.withColumn("gold_layer",    lit("gold"))
    df = df.withColumn("is_current",    lit(1))
    df = df.withColumn("effective_from",lit(now_ts))
    df = df.withColumn("effective_to",  lit("9999-12-31"))

    result = DynamicFrame.fromDF(df, glueContext, "dim_branch")
    return DynamicFrameCollection({"dim_branch": result}, glueContext)

─────────────────────────────────────────────────
A4. TARGET NODE — Gold S3 + Catalog
─────────────────────────────────────────────────
→ "+" → Target → Amazon S3

  Node name:          Target_Gold_dim_branch
  Data source:        Transform_dim_branch
  Format:             Parquet
  Compression:        Snappy
  S3 Target Location: s3://ameriprise-bank-datalake/gold/dim_branch/
  Data Catalog update options:
    → Create a table in the Data Catalog
    → Database:    ameriprise_bank_db
    → Table name:  gold_dim_branch
  Partition keys: (none — only 5 rows, no partition needed)

→ Save → Run


════════════════════════════════════════════════
PART B — GLUE VISUAL ETL JOB 2: silver_to_gold_dim_customer
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/customers/ → joins with gold/dim_branch/ to add branch_sk
  → adds customer_sk surrogate key → writes gold/dim_customer/

─────────────────────────────────────────────────
B1. JOB DETAILS
─────────────────────────────────────────────────
  Name:         silver_to_gold_dim_customer
  IAM Role:     AmerispriseBankGlueRole
  Worker type:  G.1X
  Workers:      2

Job parameters:
  --silver_customers_path   s3://ameriprise-bank-datalake/silver/customers/
  --gold_dim_branch_path    s3://ameriprise-bank-datalake/gold/dim_branch/
  --gold_dim_customer_path  s3://ameriprise-bank-datalake/gold/dim_customer/
  --table_name              dim_customer

─────────────────────────────────────────────────
B2. SOURCE NODE 1 — Silver Customers
─────────────────────────────────────────────────
  Node name:  Source_Silver_Customers
  S3 URL:     s3://ameriprise-bank-datalake/silver/customers/
  Format:     Parquet
  Recursive:  ✓

Columns:
  customer_id (int), first_name (string), last_name (string),
  date_of_birth (string), kyc_status (string), branch_code (string),
  created_at (string), updated_at (string),
  pan_masked (string), email_masked (string), phone_masked (string),
  silver_load_ts (string), dq_status (string), + other metadata cols

─────────────────────────────────────────────────
B3. SOURCE NODE 2 — Gold dim_branch (for branch_sk lookup)
─────────────────────────────────────────────────
→ Add a SECOND Source node on the canvas

  Node name:  Source_Gold_dim_branch
  S3 URL:     s3://ameriprise-bank-datalake/gold/dim_branch/
  Format:     Parquet
  Recursive:  ✓

Columns: branch_sk (int), branch_code (string), branch_name, city, state, region

─────────────────────────────────────────────────
B4. JOIN NODE — link customers to branch_sk
─────────────────────────────────────────────────
→ "+" → Transform → Join

  Node name:   Join_Customer_Branch
  Data sources: Source_Silver_Customers  AND  Source_Gold_dim_branch

  Join type:   Left join
  Join conditions:
    Left key:   branch_code   (from Source_Silver_Customers)
    Right key:  branch_code   (from Source_Gold_dim_branch)

─────────────────────────────────────────────────
B5. CUSTOM TRANSFORM NODE — Build dim_customer
─────────────────────────────────────────────────
  Node name:   Transform_dim_customer
  Data source: Join_Customer_Branch

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import (
        col, lit, row_number, coalesce, concat,
        substring, when
    )
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 1. Add surrogate key
    window = Window.orderBy("customer_id")
    df = df.withColumn("customer_sk", row_number().over(window))

    # 2. Build full_name
    df = df.withColumn("full_name",
        concat(col("first_name"), lit(" "), col("last_name"))
    )

    # 3. Select Gold dim columns only
    df = df.select(
        col("customer_sk"),
        col("customer_id"),
        col("first_name"),
        col("last_name"),
        col("full_name"),
        col("date_of_birth"),
        col("kyc_status"),
        col("branch_code"),
        # branch_sk from joined dim_branch (may be named branch_sk or `.branch_sk`)
        coalesce(col("branch_sk"), lit(0)).alias("branch_sk"),
        col("pan_masked"),
        col("email_masked"),
        col("phone_masked"),
        col("created_at").alias("source_created_at"),
        col("updated_at").alias("source_updated_at"),
    )

    # 4. Gold metadata
    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts",  lit(now_ts))
    df = df.withColumn("gold_layer",    lit("gold"))
    df = df.withColumn("is_current",    lit(1))
    df = df.withColumn("effective_from",lit(now_ts))
    df = df.withColumn("effective_to",  lit("9999-12-31"))

    result = DynamicFrame.fromDF(df, glueContext, "dim_customer")
    return DynamicFrameCollection({"dim_customer": result}, glueContext)

─────────────────────────────────────────────────
B6. TARGET NODE
─────────────────────────────────────────────────
  S3 Location: s3://ameriprise-bank-datalake/gold/dim_customer/
  Table name:  gold_dim_customer
  Partition:   kyc_status   ← partition by KYC status for fast filtering

→ Save → Run


════════════════════════════════════════════════
PART C — GLUE VISUAL ETL JOB 3: silver_to_gold_dim_account
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/accounts/ → joins dim_customer (for customer_sk) and
  dim_branch (for branch_sk) → builds gold/dim_account/

─────────────────────────────────────────────────
C1. JOB DETAILS
─────────────────────────────────────────────────
  Name:        silver_to_gold_dim_account
  IAM Role:    AmerispriseBankGlueRole
  Workers:     2

Job parameters:
  --silver_accounts_path    s3://ameriprise-bank-datalake/silver/accounts/
  --gold_dim_customer_path  s3://ameriprise-bank-datalake/gold/dim_customer/
  --gold_dim_branch_path    s3://ameriprise-bank-datalake/gold/dim_branch/
  --gold_dim_account_path   s3://ameriprise-bank-datalake/gold/dim_account/

─────────────────────────────────────────────────
C2. SOURCE NODES (3 sources)
─────────────────────────────────────────────────
Source 1:
  Node name:  Source_Silver_Accounts
  S3 URL:     s3://ameriprise-bank-datalake/silver/accounts/
  Columns: account_id, customer_id, account_type, balance,
           currency, branch_code, status, opened_date, + metadata

Source 2:
  Node name:  Source_Gold_dim_customer
  S3 URL:     s3://ameriprise-bank-datalake/gold/dim_customer/
  Columns: customer_sk, customer_id, branch_code, kyc_status

Source 3:
  Node name:  Source_Gold_dim_branch
  S3 URL:     s3://ameriprise-bank-datalake/gold/dim_branch/
  Columns: branch_sk, branch_code, city, region

─────────────────────────────────────────────────
C3. JOIN NODES
─────────────────────────────────────────────────
Join 1:
  Node name:   Join_Account_Customer
  Sources:     Source_Silver_Accounts + Source_Gold_dim_customer
  Join type:   Left join
  Left key:    customer_id   Right key: customer_id

Join 2:
  Node name:   Join_Account_Branch
  Sources:     Join_Account_Customer + Source_Gold_dim_branch
  Join type:   Left join
  Left key:    branch_code   Right key: branch_code

─────────────────────────────────────────────────
C4. CUSTOM TRANSFORM NODE
─────────────────────────────────────────────────
  Node name:   Transform_dim_account

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, lit, row_number, coalesce, round as spark_round
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 1. Surrogate key
    window = Window.orderBy("account_id")
    df = df.withColumn("account_sk", row_number().over(window))

    # 2. Round balance
    df = df.withColumn("balance", spark_round(col("balance").cast("double"), 2))

    # 3. Select Gold columns
    df = df.select(
        col("account_sk"),
        col("account_id"),
        coalesce(col("customer_sk"), lit(0)).alias("customer_sk"),
        col("account_type"),
        col("balance"),
        col("currency"),
        col("branch_code"),
        coalesce(col("branch_sk"), lit(0)).alias("branch_sk"),
        col("status"),
        col("opened_date"),
        col("created_at").alias("source_created_at"),
    )

    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts",  lit(now_ts))
    df = df.withColumn("gold_layer",    lit("gold"))
    df = df.withColumn("is_current",    lit(1))
    df = df.withColumn("effective_from",lit(now_ts))
    df = df.withColumn("effective_to",  lit("9999-12-31"))

    result = DynamicFrame.fromDF(df, glueContext, "dim_account")
    return DynamicFrameCollection({"dim_account": result}, glueContext)

─────────────────────────────────────────────────
C5. TARGET NODE
─────────────────────────────────────────────────
  S3:        s3://ameriprise-bank-datalake/gold/dim_account/
  Table:     gold_dim_account
  Partition: account_type   (Savings / Current)


════════════════════════════════════════════════
PART D — GLUE VISUAL ETL JOB 4: silver_to_gold_fact_transactions
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/transactions/ → joins all 3 dimension tables to get
  surrogate keys → writes gold/fact_transactions/
  This is the MAIN FACT TABLE — grain: one row per transaction

─────────────────────────────────────────────────
D1. JOB DETAILS
─────────────────────────────────────────────────
  Name:        silver_to_gold_fact_transactions
  IAM Role:    AmerispriseBankGlueRole
  Worker type: G.1X
  Workers:     4    ← more workers for 30000+ rows

Job parameters:
  --silver_txn_path        s3://ameriprise-bank-datalake/silver/transactions/
  --gold_dim_account_path  s3://ameriprise-bank-datalake/gold/dim_account/
  --gold_dim_date_path     s3://ameriprise-bank-datalake/gold/dim_date/
  --gold_dim_branch_path   s3://ameriprise-bank-datalake/gold/dim_branch/
  --gold_fact_txn_path     s3://ameriprise-bank-datalake/gold/fact_transactions/

─────────────────────────────────────────────────
D2. SOURCE NODES (4 sources)
─────────────────────────────────────────────────
Source 1:
  Node name: Source_Silver_Transactions
  S3 URL:    s3://ameriprise-bank-datalake/silver/transactions/
  Columns: txn_id, account_id, txn_type, amount, txn_timestamp,
           channel, status, txn_date, + metadata

Source 2:
  Node name: Source_Gold_dim_account
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_account/
  Columns: account_sk, account_id, customer_sk, branch_sk

Source 3:
  Node name: Source_Gold_dim_date
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_date/
  Columns: date_sk, full_date, year, month, quarter

Source 4:
  Node name: Source_Gold_dim_branch
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_branch/
  Columns: branch_sk, branch_code, city, region

─────────────────────────────────────────────────
D3. JOIN NODES
─────────────────────────────────────────────────
Join 1 — Transactions to Account:
  Node name:  Join_Txn_Account
  Sources:    Source_Silver_Transactions + Source_Gold_dim_account
  Join type:  Left join
  Left key:   account_id    Right key: account_id

Join 2 — to Date dimension:
  Node name:  Join_Txn_Date
  Sources:    Join_Txn_Account + Source_Gold_dim_date
  Join type:  Left join
  Left key:   txn_date      Right key: full_date
  NOTE: txn_date in silver is "YYYY-MM-DD" string format
        full_date in dim_date is also "YYYY-MM-DD" — they match directly

─────────────────────────────────────────────────
D4. CUSTOM TRANSFORM NODE
─────────────────────────────────────────────────
  Node name:   Transform_fact_transactions

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import (
        col, lit, row_number, coalesce,
        round as spark_round, substring
    )
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # 1. Surrogate key for fact table
    window = Window.orderBy("txn_id")
    df = df.withColumn("txn_sk", row_number().over(window))

    # 2. Standardize amount
    df = df.withColumn("amount", spark_round(col("amount").cast("double"), 2))

    # 3. Derive debit/credit amount columns (useful for BI)
    from pyspark.sql.functions import when
    df = df.withColumn("debit_amount",
        when(col("txn_type") == "Debit", col("amount")).otherwise(lit(0.0))
    )
    df = df.withColumn("credit_amount",
        when(col("txn_type") == "Credit", col("amount")).otherwise(lit(0.0))
    )

    # 4. Select fact table columns with surrogate keys
    df = df.select(
        col("txn_sk"),
        col("txn_id"),
        coalesce(col("account_sk"),  lit(0)).alias("account_sk"),
        coalesce(col("customer_sk"), lit(0)).alias("customer_sk"),
        coalesce(col("branch_sk"),   lit(0)).alias("branch_sk"),
        coalesce(col("date_sk"),     lit(0)).alias("date_sk"),
        col("txn_type"),
        col("amount"),
        col("debit_amount"),
        col("credit_amount"),
        col("txn_timestamp"),
        col("txn_date"),
        col("channel"),
        col("status"),
    )

    # 5. Gold metadata
    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts", lit(now_ts))
    df = df.withColumn("gold_layer",   lit("gold"))

    result = DynamicFrame.fromDF(df, glueContext, "fact_transactions")
    return DynamicFrameCollection({"fact_transactions": result}, glueContext)

─────────────────────────────────────────────────
D5. TARGET NODE
─────────────────────────────────────────────────
  S3:        s3://ameriprise-bank-datalake/gold/fact_transactions/
  Table:     gold_fact_transactions
  Partition: txn_date   ← partition by date for fast time-range queries


════════════════════════════════════════════════
PART E — GLUE VISUAL ETL JOB 5: silver_to_gold_fact_payments
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/payment_gateway_logs/ → joins dim_date → writes gold/fact_payments/
  Grain: one row per payment gateway event

─────────────────────────────────────────────────
E1. JOB DETAILS
─────────────────────────────────────────────────
  Name:        silver_to_gold_fact_payments
  IAM Role:    AmerispriseBankGlueRole
  Workers:     2

Job parameters:
  --silver_payments_path   s3://ameriprise-bank-datalake/silver/payment_gateway_logs/
  --gold_dim_date_path     s3://ameriprise-bank-datalake/gold/dim_date/
  --gold_fact_payment_path s3://ameriprise-bank-datalake/gold/fact_payments/

─────────────────────────────────────────────────
E2. SOURCE NODES
─────────────────────────────────────────────────
Source 1:
  Node name: Source_Silver_Payments
  S3 URL:    s3://ameriprise-bank-datalake/silver/payment_gateway_logs/
  Columns: txn_id, gateway_name, gateway_status, response_code,
           processing_time_ms, device_type, geo_location,
           processed_timestamp, txn_date, + metadata

Source 2:
  Node name: Source_Gold_dim_date (for payments)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_date/
  Columns:   date_sk, full_date

─────────────────────────────────────────────────
E3. JOIN NODE
─────────────────────────────────────────────────
  Node name:  Join_Payment_Date
  Sources:    Source_Silver_Payments + Source_Gold_dim_date
  Join type:  Left join
  Left key:   txn_date      Right key: full_date

─────────────────────────────────────────────────
E4. CUSTOM TRANSFORM
─────────────────────────────────────────────────
  Node name: Transform_fact_payments

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, lit, row_number, coalesce, when
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    # Surrogate key
    window = Window.orderBy("txn_id")
    df = df.withColumn("payment_sk", row_number().over(window))

    # Flag for success/failure (useful for BI aggregations)
    df = df.withColumn("is_success",
        when(col("gateway_status") == "SUCCESS", lit(1)).otherwise(lit(0))
    )

    df = df.select(
        col("payment_sk"),
        col("txn_id"),
        coalesce(col("date_sk"), lit(0)).alias("date_sk"),
        col("gateway_name"),
        col("gateway_status"),
        col("response_code"),
        col("processing_time_ms"),
        col("device_type"),
        col("geo_location"),
        col("processed_timestamp"),
        col("txn_date"),
        col("is_success"),
    )

    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts", lit(now_ts))
    df = df.withColumn("gold_layer",   lit("gold"))

    result = DynamicFrame.fromDF(df, glueContext, "fact_payments")
    return DynamicFrameCollection({"fact_payments": result}, glueContext)

─────────────────────────────────────────────────
E5. TARGET NODE
─────────────────────────────────────────────────
  S3:        s3://ameriprise-bank-datalake/gold/fact_payments/
  Table:     gold_fact_payments
  Partition: txn_date


════════════════════════════════════════════════
PART F — GLUE VISUAL ETL JOB 6: silver_to_gold_fact_credit_risk
════════════════════════════════════════════════

WHAT IT DOES:
  Reads silver/credit_bureau_reports/ → joins dim_customer and dim_date
  → writes gold/fact_credit_risk/
  Grain: one row per customer per bureau_pull_date (monthly snapshot)

─────────────────────────────────────────────────
F1. JOB DETAILS
─────────────────────────────────────────────────
  Name:        silver_to_gold_fact_credit_risk
  IAM Role:    AmerispriseBankGlueRole
  Workers:     2

Job parameters:
  --silver_credit_path        s3://ameriprise-bank-datalake/silver/credit_bureau_reports/
  --gold_dim_customer_path    s3://ameriprise-bank-datalake/gold/dim_customer/
  --gold_dim_date_path        s3://ameriprise-bank-datalake/gold/dim_date/
  --gold_fact_credit_path     s3://ameriprise-bank-datalake/gold/fact_credit_risk/

─────────────────────────────────────────────────
F2. SOURCE NODES
─────────────────────────────────────────────────
Source 1:
  Node name: Source_Silver_CreditBureau
  S3 URL:    s3://ameriprise-bank-datalake/silver/credit_bureau_reports/
  Columns: customer_id, credit_score, risk_grade, risk_band,
           external_active_loans, external_overdue_amount,
           bureau_pull_date, + metadata

Source 2:
  Node name: Source_Gold_dim_customer (for credit)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_customer/
  Columns:   customer_sk, customer_id

Source 3:
  Node name: Source_Gold_dim_date (for credit)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_date/
  Columns:   date_sk, full_date

─────────────────────────────────────────────────
F3. JOIN NODES
─────────────────────────────────────────────────
Join 1:
  Node name:  Join_Credit_Customer
  Sources:    Source_Silver_CreditBureau + Source_Gold_dim_customer
  Join type:  Left join
  Left key:   customer_id    Right key: customer_id

Join 2:
  Node name:  Join_Credit_Date
  Sources:    Join_Credit_Customer + Source_Gold_dim_date
  Join type:  Left join
  Left key:   bureau_pull_date    Right key: full_date

─────────────────────────────────────────────────
F4. CUSTOM TRANSFORM
─────────────────────────────────────────────────
  Node name: Transform_fact_credit_risk

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, lit, row_number, coalesce, round as spark_round
    from pyspark.sql.window import Window

    df = dfc.select(list(dfc.keys())[0]).toDF()

    window = Window.orderBy("customer_id", "bureau_pull_date")
    df = df.withColumn("credit_sk", row_number().over(window))

    df = df.withColumn("external_overdue_amount",
        spark_round(col("external_overdue_amount").cast("double"), 2)
    )

    df = df.select(
        col("credit_sk"),
        coalesce(col("customer_sk"), lit(0)).alias("customer_sk"),
        col("customer_id"),
        coalesce(col("date_sk"), lit(0)).alias("date_sk"),
        col("credit_score"),
        col("risk_grade"),
        col("risk_band"),
        col("external_active_loans"),
        col("external_overdue_amount"),
        col("bureau_pull_date"),
    )

    now_ts = datetime.now(timezone.utc).isoformat()
    df = df.withColumn("gold_load_ts", lit(now_ts))
    df = df.withColumn("gold_layer",   lit("gold"))

    result = DynamicFrame.fromDF(df, glueContext, "fact_credit_risk")
    return DynamicFrameCollection({"fact_credit_risk": result}, glueContext)

─────────────────────────────────────────────────
F5. TARGET NODE
─────────────────────────────────────────────────
  S3:        s3://ameriprise-bank-datalake/gold/fact_credit_risk/
  Table:     gold_fact_credit_risk
  Partition: bureau_pull_date


════════════════════════════════════════════════
PART G — GLUE VISUAL ETL JOB 7: silver_to_gold_aggregations
════════════════════════════════════════════════

WHAT IT DOES:
  Reads gold/fact_transactions/ → computes 4 aggregation tables
  These are pre-computed summaries for fast BI dashboard queries.
  No joins needed — all keys already in fact_transactions.

─────────────────────────────────────────────────
G1. JOB DETAILS
─────────────────────────────────────────────────
  Name:        silver_to_gold_aggregations
  IAM Role:    AmerispriseBankGlueRole
  Workers:     4

Job parameters:
  --gold_fact_txn_path           s3://ameriprise-bank-datalake/gold/fact_transactions/
  --gold_dim_branch_path         s3://ameriprise-bank-datalake/gold/dim_branch/
  --gold_dim_customer_path       s3://ameriprise-bank-datalake/gold/dim_customer/
  --gold_dim_account_path        s3://ameriprise-bank-datalake/gold/dim_account/
  --gold_agg_daily_path          s3://ameriprise-bank-datalake/gold/agg_daily_balances/
  --gold_agg_monthly_path        s3://ameriprise-bank-datalake/gold/agg_monthly_summary/
  --gold_agg_branch_path         s3://ameriprise-bank-datalake/gold/agg_branch_performance/
  --gold_agg_customer360_path    s3://ameriprise-bank-datalake/gold/agg_customer_360/

─────────────────────────────────────────────────
G2. SOURCE NODES
─────────────────────────────────────────────────
Source 1:
  Node name: Source_Gold_fact_transactions
  S3 URL:    s3://ameriprise-bank-datalake/gold/fact_transactions/
  Columns: txn_sk, txn_id, account_sk, customer_sk, branch_sk,
           date_sk, txn_type, amount, debit_amount, credit_amount,
           txn_date, channel, status

Source 2:
  Node name: Source_Gold_dim_branch (for agg)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_branch/
  Columns:   branch_sk, branch_code, branch_name, city, region

Source 3:
  Node name: Source_Gold_dim_customer (for agg)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_customer/
  Columns:   customer_sk, customer_id, full_name, kyc_status, branch_sk

Source 4:
  Node name: Source_Gold_dim_account (for agg)
  S3 URL:    s3://ameriprise-bank-datalake/gold/dim_account/
  Columns:   account_sk, account_id, account_type, balance, status

─────────────────────────────────────────────────
G3. JOIN NODES
─────────────────────────────────────────────────
Join 1:
  Node name:  Join_Fact_Branch
  Sources:    Source_Gold_fact_transactions + Source_Gold_dim_branch
  Type:       Left join
  Keys:       branch_sk = branch_sk

Join 2:
  Node name:  Join_Fact_Account
  Sources:    Join_Fact_Branch + Source_Gold_dim_account
  Type:       Left join
  Keys:       account_sk = account_sk

─────────────────────────────────────────────────
G4. CUSTOM TRANSFORM — All 4 aggregations in one job
─────────────────────────────────────────────────
  Node name: Transform_all_aggregations

Paste this code:

def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import (
        col, lit, sum as spark_sum, count as spark_count,
        avg as spark_avg, max as spark_max, min as spark_min,
        round as spark_round, substring, countDistinct
    )

    df = dfc.select(list(dfc.keys())[0]).toDF()
    now_ts = datetime.now(timezone.utc).isoformat()

    # ── AGG 1: Daily Balances per Account ──────────────────────
    # Shows net movement (credit - debit) per account per day
    agg_daily = df.groupBy("account_sk", "account_id", "txn_date").agg(
        spark_sum("credit_amount").alias("total_credit"),
        spark_sum("debit_amount").alias("total_debit"),
        (spark_sum("credit_amount") - spark_sum("debit_amount")).alias("net_balance_change"),
        spark_count("txn_id").alias("txn_count"),
        spark_avg("amount").alias("avg_txn_amount"),
    )
    agg_daily = agg_daily.withColumn("gold_load_ts", lit(now_ts))
    agg_daily = agg_daily.withColumn("agg_type", lit("daily_balances"))

    # ── AGG 2: Monthly Branch Summary ─────────────────────────
    # Monthly totals per branch — for branch performance tracking
    agg_monthly = df.withColumn("year_month", substring(col("txn_date"), 1, 7))
    agg_monthly = agg_monthly.groupBy("branch_sk", "branch_code", "branch_name", "year_month").agg(
        spark_sum("amount").alias("total_txn_volume"),
        spark_count("txn_id").alias("total_txn_count"),
        spark_sum("credit_amount").alias("total_credits"),
        spark_sum("debit_amount").alias("total_debits"),
        spark_avg("amount").alias("avg_txn_amount"),
        countDistinct("account_sk").alias("active_accounts"),
    )
    agg_monthly = agg_monthly.withColumn("gold_load_ts", lit(now_ts))
    agg_monthly = agg_monthly.withColumn("agg_type", lit("monthly_branch_summary"))

    # ── AGG 3: Branch Performance (all-time KPIs) ─────────────
    agg_branch = df.groupBy("branch_sk", "branch_code", "branch_name", "city", "region").agg(
        spark_sum("amount").alias("total_txn_volume"),
        spark_count("txn_id").alias("total_txn_count"),
        spark_avg("amount").alias("avg_txn_amount"),
        spark_max("amount").alias("max_single_txn"),
        spark_min("txn_date").alias("first_txn_date"),
        spark_max("txn_date").alias("last_txn_date"),
        countDistinct("account_sk").alias("unique_accounts"),
        countDistinct("customer_sk").alias("unique_customers"),
    )
    agg_branch = agg_branch.withColumn("gold_load_ts", lit(now_ts))
    agg_branch = agg_branch.withColumn("agg_type", lit("branch_performance"))

    # ── AGG 4: Customer 360 View ──────────────────────────────
    # One row per customer with all-time transaction summary
    agg_cust360 = df.groupBy("customer_sk").agg(
        spark_sum("amount").alias("lifetime_txn_volume"),
        spark_count("txn_id").alias("lifetime_txn_count"),
        spark_avg("amount").alias("avg_txn_amount"),
        spark_sum("credit_amount").alias("total_credits_received"),
        spark_sum("debit_amount").alias("total_debits_made"),
        spark_max("amount").alias("largest_single_txn"),
        spark_min("txn_date").alias("first_txn_date"),
        spark_max("txn_date").alias("last_txn_date"),
        countDistinct("txn_date").alias("active_days"),
        countDistinct("account_sk").alias("num_accounts"),
    )
    agg_cust360 = agg_cust360.withColumn("gold_load_ts", lit(now_ts))
    agg_cust360 = agg_cust360.withColumn("agg_type", lit("customer_360"))

    result1 = DynamicFrame.fromDF(agg_daily,    glueContext, "agg_daily_balances")
    result2 = DynamicFrame.fromDF(agg_monthly,  glueContext, "agg_monthly_summary")
    result3 = DynamicFrame.fromDF(agg_branch,   glueContext, "agg_branch_performance")
    result4 = DynamicFrame.fromDF(agg_cust360,  glueContext, "agg_customer_360")

    return DynamicFrameCollection({
        "agg_daily_balances":    result1,
        "agg_monthly_summary":   result2,
        "agg_branch_performance":result3,
        "agg_customer_360":      result4,
    }, glueContext)

─────────────────────────────────────────────────
G5. TARGET NODES (4 separate targets — one per aggregation)
─────────────────────────────────────────────────
Because the custom transform returns 4 DataFrames,
you need 4 separate Target nodes, each connected to its output:

Target 1:
  Source output key: agg_daily_balances
  S3:    s3://ameriprise-bank-datalake/gold/agg_daily_balances/
  Table: gold_agg_daily_balances
  Partition: txn_date

Target 2:
  Source output key: agg_monthly_summary
  S3:    s3://ameriprise-bank-datalake/gold/agg_monthly_summary/
  Table: gold_agg_monthly_summary
  Partition: year_month

Target 3:
  Source output key: agg_branch_performance
  S3:    s3://ameriprise-bank-datalake/gold/agg_branch_performance/
  Table: gold_agg_branch_performance
  Partition: (none — one row per branch, only 5 rows)

Target 4:
  Source output key: agg_customer_360
  S3:    s3://ameriprise-bank-datalake/gold/agg_customer_360/
  Table: gold_agg_customer_360
  Partition: (none)

→ Save → Run


════════════════════════════════════════════════
CONSOLE VERIFICATION AFTER ALL 7 JOBS
════════════════════════════════════════════════

1. GLUE JOBS:
   → Glue → ETL Jobs → all 7 show: Succeeded

2. S3 GOLD ZONE:
   → S3 → ameriprise-bank-datalake → gold/
   → Should see ALL of:
     dim_branch/               ← 5 rows
     dim_customer/             ← 500+ rows
     dim_account/              ← 1000+ rows
     dim_date/                 ← 3650 rows (2020-2030)
     fact_transactions/        ← 30000+ rows (partitioned by txn_date)
     fact_payments/            ← 20000 rows
     fact_credit_risk/         ← 5500 rows
     agg_daily_balances/       ← one row per account per day
     agg_monthly_summary/      ← one row per branch per month
     agg_branch_performance/   ← 5 rows (one per branch)
     agg_customer_360/         ← 500+ rows (one per customer)

3. GLUE DATA CATALOG:
   → Glue → Data Catalog → Tables → filter: ameriprise_bank_db
   → Should see all gold_ prefixed tables registered

4. QUICK QUERY WITH ATHENA (optional but powerful):
   → Go to: https://console.aws.amazon.com/athena
   → Database: ameriprise_bank_db
   → Run: SELECT COUNT(*) FROM gold_fact_transactions;
   → Should return: 30000+ rows instantly


════════════════════════════════════════════════
TROUBLESHOOTING
════════════════════════════════════════════════

ERROR: "AnalysisException: resolved attribute(s) branch_sk missing"
CAUSE: Column name conflict after join (both tables have branch_sk)
FIX:   In custom transform, use coalesce() to handle ambiguous cols:
       coalesce(col("branch_sk"), lit(0)).alias("branch_sk")

ERROR: "Join output is empty — 0 rows after join"
CAUSE: Key mismatch — date format in fact vs dim_date
FIX:   Ensure txn_date in silver is "YYYY-MM-DD" string
       and full_date in dim_date is also "YYYY-MM-DD"
       dim_date is created by step1_create_dim_date.py — check that ran

ERROR: "Multiple sources found for Custom Transform"
CAUSE: Wrong data source selected in custom transform node
FIX:   Click the custom transform node → right panel →
       Data source: select only the immediately upstream node

ERROR: "4 targets from 1 Custom Transform not connecting"
CAUSE: Visual ETL handles multi-output differently
FIX:   After creating the 4-output custom transform:
       → Each Target node: click it → Data source dropdown
       → Select the specific output key name
         (agg_daily_balances / agg_monthly_summary etc.)

ERROR: "Access denied writing to gold/"
FIX:   Check IAM Role → AmerispriseBankGlueRole has AmazonS3FullAccess
       OR verify the specific gold/ prefix is included in inline policy


════════════════════════════════════════════════
PROMPT TO START PHASE 6 (copy-paste to new chat)
════════════════════════════════════════════════

I am building an AWS Data Engineering project for Ameriprise Bank.
Completed phases:
  Phase 2: S3 Data Lake — bronze/silver/gold/quarantine/metadata zones
  Phase 3: RDS SQL Server — 4 tables (branches/customers/accounts/transactions)
  Phase 4: Glue Visual ETL — 6 DQ jobs, silver layer populated with PII masked
  Phase 5: Glue Visual ETL — 7 Gold jobs, star schema built:
           4 dims (dim_branch/dim_customer/dim_account/dim_date)
           3 facts (fact_transactions/fact_payments/fact_credit_risk)
           4 aggs (daily_balances/monthly_summary/branch_perf/customer_360)

S3 Bucket: ameriprise-bank-datalake  Region: ap-south-1  OS: Ubuntu

Give me Phase 6 — Redshift Serverless setup + load Gold layer +
connect Power BI. Same depth as previous phases. Everything included.
