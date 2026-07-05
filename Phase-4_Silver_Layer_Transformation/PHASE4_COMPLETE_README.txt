PHASE 4 — COMPLETE IMPLEMENTATION GUIDE
AWS Glue Visual ETL — Data Quality Engine (Bronze → Silver)
Ameriprise Bank Data Engineering Project
================================================

QUICK REVISION
═══════════════
Phase 2 ✅  S3 Data Lake — bucket: neo-bank-datalake (ap-south-1)
Phase 3 ✅  RDS SQL Server — 4 banking tables extracted to bronze
Phase 4 ⬅️  THIS PHASE — 6 Glue Visual ETL jobs (Bronze → Silver)
Phase 5     Gold Layer Star Schema (next)
Phase 6     Redshift + Power BI (next)


WHAT PHASE 4 BUILDS
════════════════════
Reads bronze/ Parquet files → runs Data Quality checks
→ PASS rows go to silver/ (cleaned, PII masked)
→ FAIL rows go to quarantine/ (with fail reason)
→ DQ rule audit log goes to metadata/dq_results/

6 GLUE VISUAL ETL JOBS:
  Job 1: bronze_to_silver_customers
  Job 2: bronze_to_silver_accounts
  Job 3: bronze_to_silver_transactions
  Job 4: bronze_to_silver_payment_gateway
  Job 5: bronze_to_silver_credit_bureau
  Job 6: bronze_to_silver_branches


PRE-REQUISITES
═══════════════

ONE-TIME SETUP IN AWS CONSOLE:

STEP 1 — Create IAM Role for Glue:
  IAM Console → Roles → Create role
  → Trusted entity: AWS service → Glue
  → Attach policies:
      AmazonS3FullAccess
      AWSGlueServiceRole
      CloudWatchFullAccess
  → Role name: AmerispriseBankGlueRole
  → Create

STEP 2 — Create Glue Database:
  Glue Console → Databases → Add database
  → Name: noe_bank_db
  → Description: Ameriprise Bank Data Lake Catalog
  → Create

STEP 3 — Create and Run Bronze Crawler:
  Glue Console → Crawlers → Create crawler
  → Name: neo-bank-bronze-crawler
  → S3 path: s3://neo-bank-datalake/bronze/
  → Exclusion patterns (CRITICAL):
      **/.keep
      **.keep
      *historical*
      *incremental*
  → IAM Role: AmerispriseBankGlueRole
  → Database: noe_bank_db
  → Prefix: bronze_
  → Run crawler
  → Verify 6 tables created (no .keep tables, no historical/incremental
    duplicate tables)


COMMON CANVAS PATTERN (ALL 6 JOBS)
════════════════════════════════════

Every job uses this same canvas structure:

  [Source S3 — bronze/tablename/]
              ↓
       [DQ_Checks_TableName]
        ↓                    ↓
  [rowLevelOutcomes]      [ruleOutcomes]
        ↓                       ↓
  [Custom Transform —      [Target S3 —
   Silver + Quarantine]    metadata/dq_results/]
        ↓             ↓
  [SelectColl       [SelectColl
    _Silver]         _Quarantine]
   index = 0          index = 1
        ↓                ↓
  [Target Silver]   [Target Quarantine]
   silver/tname/    quarantine/tname/

NOTE: Branches has NO Quarantine target — only Silver.
       So for branches: only one Custom Transform output (Index 0).


KEY VISUAL ETL CONFIGURATIONS (APPLIES TO ALL JOBS)
═════════════════════════════════════════════════════

DQ NODE — IMPORTANT SETTINGS:
  ✓ Original data
  ✓ Add new columns to indicate data quality errors  ← MUST BE CHECKED
  ✓ Data quality results
  On ruleset failure: Continue with job
  Publish to CloudWatch: ✓

  This gives you 2 output ports:
    rowLevelOutcomes   → contains DataQualityEvaluationResult column
    ruleOutcomes       → contains rule pass/fail summary

  IF YOU UNCHECK "Add new columns" → output key becomes "originalData"
  AND DataQualityEvaluationResult column will NOT exist
  → PASS/FAIL split breaks!

CUSTOM TRANSFORM — FUNCTION NAME CONFLICT:
  Glue Visual ETL auto-names every Custom Transform function as MyTransform
  If you create 2 Custom Transform nodes in one job → name collision
  Solution: ONE Custom Transform that handles BOTH Silver + Quarantine
  Then use 2 Select From Collection nodes (index 0 and 1) to split outputs

DATA PREVIEW SHOWS 0 ROWS:
  Multi-output Custom Transforms always show 0 rows in data preview
  This is a Visual ETL display limitation, NOT a real bug
  Run the job and verify in S3 — actual output is correct


JOB 1 — bronze_to_silver_customers
════════════════════════════════════

NODE 1: Source S3
  S3 URL:  s3://neo-bank-datalake/bronze/customers/
  Format:  Parquet, Recursive ✓

NODE 2: DQ Checks (paste in DQDL editor):
Rules = [
    Completeness "customer_id" >= 1.0,
    Uniqueness "customer_id" >= 1.0,
    Completeness "kyc_status" >= 0.95,
    ColumnValues "kyc_status" in ["VERIFIED", "PENDING", "REJECTED"],
    ColumnLength "phone_number" = 10
]

NODE 3: Custom Transform (Silver + Quarantine combined):
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import (
        col, lit, when, upper, trim,
        concat, substring, regexp_replace
    )
    from datetime import datetime, timezone

    df = dfc.select(list(dfc.keys())[0]).toDF()
    now_ts = datetime.now(timezone.utc).isoformat()

    if "DataQualityEvaluationResult" in df.columns:
        passed_df = df.filter(col("DataQualityEvaluationResult") == "Passed")
        failed_df = df.filter(col("DataQualityEvaluationResult") != "Passed")
    else:
        passed_df = df
        failed_df = df.filter(lit(False))

    # PII Masking
    passed_df = passed_df.withColumn("pan_masked",
        when(col("pan_number").isNull(), lit("UNKNOWN"))
        .otherwise(concat(substring(col("pan_number"), 1, 5), lit("*****")))
    )
    passed_df = passed_df.withColumn("email_masked",
        when(col("email").isNull(), lit(None))
        .otherwise(regexp_replace(col("email"), r"^[^@]+", "***"))
    )
    passed_df = passed_df.withColumn("phone_masked",
        when(col("phone_number").isNull(), lit(None))
        .otherwise(concat(lit("XXXXXX"), substring(col("phone_number"), 7, 4)))
    )

    # Standardize
    passed_df = passed_df.withColumn("kyc_status",  upper(trim(col("kyc_status"))))
    passed_df = passed_df.withColumn("first_name",  trim(col("first_name")))
    passed_df = passed_df.withColumn("last_name",   trim(col("last_name")))
    passed_df = passed_df.withColumn("branch_code", trim(col("branch_code")))

    # Drop PII + DQ columns
    passed_df = passed_df.drop("pan_number", "email", "phone_number")
    dq_cols = ["DataQualityEvaluationResult","DataQualityRulesPass","DataQualityRulesFail",
               "DataQualityRulesSkip","DataQualityRulesPassed","DataQualityRulesFailed",
               "DataQualityRulesSkipped"]
    for c in dq_cols:
        if c in passed_df.columns:
            passed_df = passed_df.drop(c)
        if c in failed_df.columns:
            failed_df = failed_df.drop(c)

    # Silver metadata
    passed_df = passed_df.withColumn("silver_load_ts", lit(now_ts))
    passed_df = passed_df.withColumn("silver_layer",   lit("silver"))
    passed_df = passed_df.withColumn("dq_status",      lit("PASSED"))

    # Quarantine metadata
    failed_df = failed_df.withColumn("quarantine_ts",     lit(now_ts))
    failed_df = failed_df.withColumn("dq_status",         lit("FAILED"))
    failed_df = failed_df.withColumn("quarantine_reason", lit("DQ_RULE_FAILED"))

    return DynamicFrameCollection({
        "silver_customers":     DynamicFrame.fromDF(passed_df, glueContext, "silver_customers"),
        "quarantine_customers": DynamicFrame.fromDF(failed_df, glueContext, "quarantine_customers"),
    }, glueContext)

TARGETS:
  ruleOutcomes  → s3://neo-bank-datalake/metadata/dq_results/   (JSON)
  Silver        → s3://neo-bank-datalake/silver/customers/      (Parquet, partition: load_date)
  Quarantine    → s3://neo-bank-datalake/quarantine/customers/  (Parquet, partition: load_date)


JOB 2 — bronze_to_silver_accounts
═══════════════════════════════════

DQ Rules:
Rules = [
    Completeness "account_id" >= 1.0,
    Uniqueness "account_id" >= 1.0,
    Completeness "customer_id" >= 1.0,
    ColumnValues "account_type" in ["Savings", "Current"],
    ColumnValues "status" in ["ACTIVE", "CLOSED", "FROZEN", "DORMANT"],
    ColumnValues "balance" >= 0,
    ColumnValues "currency" = "INR"
]

Custom Transform — standardize only (no PII):
  account_type → trim
  status       → upper + trim
  currency     → upper + trim
  balance      → round to 2 decimals
  branch_code  → trim
  Add silver_load_ts, silver_layer, dq_status

Output keys: silver_accounts, quarantine_accounts

TARGETS:
  Silver:     s3://neo-bank-datalake/silver/accounts/      partition: load_date
  Quarantine: s3://neo-bank-datalake/quarantine/accounts/  partition: load_date


JOB 3 — bronze_to_silver_transactions
═══════════════════════════════════════

Workers: 4 (more workers because 30000+ rows)

DQ Rules:
Rules = [
    Completeness "txn_id" >= 1.0,
    Uniqueness "txn_id" >= 1.0,
    Completeness "account_id" >= 1.0,
    ColumnValues "txn_type" in ["Debit", "Credit"],
    ColumnValues "amount" > 0,
    ColumnValues "status" in ["SUCCESS", "FAILED", "PENDING", "REVERSED"],
    Completeness "txn_timestamp" >= 1.0
]

Custom Transform — standardize:
  txn_type → trim
  status   → upper + trim
  channel  → upper + trim
  amount   → round to 2 decimals

Output keys: silver_transactions, quarantine_transactions

TARGETS:
  Silver:     s3://neo-bank-datalake/silver/transactions/      partition: load_date
  Quarantine: s3://neo-bank-datalake/quarantine/transactions/  partition: load_date


JOB 4 — bronze_to_silver_payment_gateway
══════════════════════════════════════════

DQ Rules:
Rules = [
    Completeness "txn_id" >= 1.0,
    Uniqueness "txn_id" >= 1.0,
    ColumnValues "gateway_name" in ["Stripe", "BillDesk", "PayU", "Razorpay"],
    ColumnValues "gateway_status" in ["SUCCESS", "FAILED", "PENDING", "TIMEOUT"],
    ColumnLength "response_code" = 2,
    ColumnValues "processing_time_ms" >= 0,
    ColumnValues "device_type" in ["Mobile", "ATM", "POS", "Web"]
]

Custom Transform — standardize:
  gateway_name   → trim
  gateway_status → upper + trim
  device_type    → trim
  geo_location   → trim
  response_code  → trim

Output keys: silver_payment_gateway, quarantine_payment_gateway

TARGETS:
  Silver:     s3://neo-bank-datalake/silver/payment_gateway_logs/      partition: load_date
  Quarantine: s3://neo-bank-datalake/quarantine/payment_gateway_logs/  partition: load_date


JOB 5 — bronze_to_silver_credit_bureau
════════════════════════════════════════

DQ Rules:
Rules = [
    Completeness "customer_id" >= 1.0,
    Completeness "credit_score" >= 1.0,
    ColumnValues "credit_score" >= 300,
    ColumnValues "credit_score" <= 900,
    ColumnValues "risk_grade" in ["LOW", "MEDIUM", "HIGH"],
    ColumnValues "external_active_loans" >= 0,
    ColumnValues "external_overdue_amount" >= 0
]

Custom Transform — KEY ENRICHMENT:
  This is the only job that adds a NEW business column:
    risk_band:
      credit_score >= 750 → "EXCELLENT"
      credit_score >= 650 → "GOOD"
      credit_score >= 550 → "FAIR"
      else                → "POOR"

Output keys: silver_credit_bureau, quarantine_credit_bureau

TARGETS:
  Silver:     s3://neo-bank-datalake/silver/credit_bureau_reports/
              partition: bureau_pull_date  ← unique partition for this table
  Quarantine: s3://neo-bank-datalake/quarantine/credit_bureau_reports/
              partition: load_date


JOB 6 — bronze_to_silver_branches (SIMPLIFIED — NO QUARANTINE)
═══════════════════════════════════════════════════════════════

This job is unique:
  Only 5 rows of data → quarantine target NOT created
  quarantine/branches/ folder DOES NOT exist in S3
  Custom Transform returns ONLY ONE output (silver_branches)

Canvas:
  [Source bronze/branches/]
            ↓
  [DQ_Checks_Branches]
       ↓               ↓
  [rowLevelOutcomes] [ruleOutcomes]
            ↓                ↓
  [Custom Transform]  [Target DQ Results]
            ↓
  [Target Silver]
   silver/branches/

DQ Rules:
Rules = [
    Completeness "branch_code" >= 1.0,
    Uniqueness "branch_code" >= 1.0,
    Completeness "branch_name" >= 1.0,
    Completeness "city" >= 1.0
]

Custom Transform (single output, PASS rows only):
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from datetime import datetime, timezone
    from awsglue.dynamicframe import DynamicFrame
    from pyspark.sql.functions import col, lit, upper, trim

    df = dfc.select(list(dfc.keys())[0]).toDF()
    now_ts = datetime.now(timezone.utc).isoformat()

    if "DataQualityEvaluationResult" in df.columns:
        df = df.filter(col("DataQualityEvaluationResult") == "Passed")

    df = df.withColumn("branch_code", trim(upper(col("branch_code"))))
    df = df.withColumn("branch_name", trim(col("branch_name")))
    df = df.withColumn("city",        trim(col("city")))
    df = df.withColumn("state",       trim(col("state")))
    df = df.withColumn("region",      trim(upper(col("region"))))

    dq_cols = ["DataQualityEvaluationResult","DataQualityRulesPass","DataQualityRulesFail",
               "DataQualityRulesSkip","DataQualityRulesPassed","DataQualityRulesFailed",
               "DataQualityRulesSkipped"]
    for c in dq_cols:
        if c in df.columns:
            df = df.drop(c)

    df = df.withColumn("silver_load_ts", lit(now_ts))
    df = df.withColumn("silver_layer",   lit("silver"))
    df = df.withColumn("dq_status",      lit("PASSED"))

    result = DynamicFrame.fromDF(df, glueContext, "silver_branches")
    return DynamicFrameCollection({"silver_branches": result}, glueContext)

TARGETS:
  ruleOutcomes  → s3://neo-bank-datalake/metadata/dq_results/   (JSON)
  Silver        → s3://neo-bank-datalake/silver/branches/       (no partition, only 5 rows)


PARTITION REFERENCE TABLE (FINAL)
═══════════════════════════════════
| Job              | Silver Partition | Quarantine Partition |
|------------------|------------------|----------------------|
| customers        | load_date        | load_date            |
| accounts         | load_date        | load_date            |
| transactions     | load_date        | load_date            |
| payment_gateway  | load_date        | load_date            |
| credit_bureau    | bureau_pull_date | load_date            |
| branches         | none             | NO QUARANTINE        |

DQ results path for ALL jobs:
  s3://neo-bank-datalake/metadata/dq_results/   (no subfolders)


COMMON ISSUES + SOLUTIONS (FROM ACTUAL EXPERIENCE)
════════════════════════════════════════════════════

ISSUE 1: Bronze crawler created 14 tables instead of 6
  CAUSE: .keep files + historical/incremental files crawled separately
  FIX:   Add exclusion patterns: **/.keep, *historical*, *incremental*
         Run merge script to combine historical + incremental files into one

ISSUE 2: "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))"
  CAUSE: Phase 3 saved datetime columns as nanosecond precision
  FIX:   Run fix_timestamp_parquet_files.py to convert to ISO strings

ISSUE 3: Filter node Column dropdown does not show DataQualityEvaluationResult
  CAUSE: Glue Visual ETL Filter node only shows columns from original schema
  FIX:   Use Custom Transform with PASS/FAIL split instead of Filter nodes

ISSUE 4: "Parent node outputs a collection, but Target does not accept a collection"
  CAUSE: Custom Transform returns DynamicFrameCollection, Target expects DataFrame
  FIX:   Add Select From Collection node between Custom Transform and Target

ISSUE 5: Two MyTransform functions overwrite each other
  CAUSE: Glue auto-names every Custom Transform function as MyTransform
  FIX:   Use ONE Custom Transform with multiple named outputs
         Use Select From Collection with index 0/1 to split

ISSUE 6: Data preview shows 0 rows for Custom Transform
  CAUSE: Multi-output transforms — Glue does not know which to display
  FIX:   This is a display limitation only. Run job and verify in S3.

ISSUE 7: S3 metadata UnicodeEncodeError em dash
  CAUSE: Em dash (—) in description string is not ASCII
  FIX:   Use ASCII-only characters in S3 object metadata

ISSUE 8: All 6 silver tables show string type for date columns
  CAUSE: Phase 3 fix saved datetime as ISO strings for Glue compatibility
  FIX:   Acceptable for Silver. Cast to proper types in Gold layer (Phase 5).


CONSOLE VERIFICATION CHECKLIST
═══════════════════════════════

After all 6 jobs complete:

  Glue Console → ETL Jobs:
    All 6 jobs show last run status: Succeeded

  S3 → neo-bank-datalake → silver/:
    silver/customers/             ← Parquet files, partitioned by load_date
    silver/accounts/              ← Parquet files, partitioned by load_date
    silver/transactions/          ← Parquet files, partitioned by load_date
    silver/payment_gateway_logs/  ← Parquet files, partitioned by load_date
    silver/credit_bureau_reports/ ← Parquet files, partitioned by bureau_pull_date
    silver/branches/              ← Parquet file (no partition)

  S3 → neo-bank-datalake → quarantine/:
    Empty for all tables (clean data) — expected for this dataset

  S3 → neo-bank-datalake → metadata/dq_results/:
    Multiple JSON files — one per job run

  Quick check via Athena:
    SELECT COUNT(*) FROM noe_bank_db.silver_customers;
    SELECT * FROM noe_bank_db.silver_credit_bureau_reports
    WHERE risk_band = 'EXCELLENT' LIMIT 5;


PHASE 4 VERIFICATION
══════════════════════
Run: python phase4_verify_silver_layer.py
Reads each silver Parquet from S3, checks row counts,
verifies metadata columns, confirms PII removed, displays quarantine stats.


PROMPT FOR PHASE 5 (paste in new chat)
═══════════════════════════════════════
I am building Ameriprise Bank AWS DE project. Completed:
  Phase 2 S3 Data Lake (neo-bank-datalake bucket, ap-south-1)
  Phase 3 RDS SQL Server with 4 banking tables
  Phase 4 6 Silver Glue Visual ETL jobs done
         silver/ has all 6 cleaned tables
Give me Phase 5: Gold Layer Star Schema (4 dims + 3 facts + 4 aggs).
End-to-end Visual ETL implementation. OS Ubuntu.
