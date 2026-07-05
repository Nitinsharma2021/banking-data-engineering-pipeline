"""
=============================================================================
PHASE 6 — STEP 5: SETUP ATHENA FOR GOLD ZONE QUERIES
=============================================================================
Purpose  : Create Glue Crawler on gold/ → register tables in catalog
           So Athena can query gold S3 Parquet directly without loading
Run      : python step5_setup_athena.py
=============================================================================
"""

import boto3
import time


AWS_REGION    = "ap-south-1"
BUCKET_NAME   = "neo-bank-datalake"
GLUE_DATABASE = "noe_bank_db"
GLUE_ROLE     = "AmerispriseBankGlueRole"
CRAWLER_NAME  = "neo-bank-gold-crawler"


def main():
    print("=" * 60)
    print("  PHASE 6 STEP 5: Setup Athena for Gold Zone")
    print("=" * 60)

    glue = boto3.client("glue", region_name=AWS_REGION)
    s3   = boto3.client("s3",   region_name=AWS_REGION)

    # Create crawler
    print(f"\n[1] Creating Glue Crawler...")
    try:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=GLUE_ROLE,
            DatabaseName=GLUE_DATABASE,
            Targets={"S3Targets": [{
                "Path": f"s3://{BUCKET_NAME}/gold/",
                "Exclusions": ["**/.keep", "**/_keep", "**/*.json"]
            }]},
            TablePrefix="gold_",
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
        )
        print(f"  [CREATED] {CRAWLER_NAME}")
    except glue.exceptions.AlreadyExistsException:
        print(f"  [EXISTS]  {CRAWLER_NAME}")

    # Run crawler
    print(f"\n[2] Running crawler...")
    glue.start_crawler(Name=CRAWLER_NAME)

    start = time.time()
    while True:
        resp  = glue.get_crawler(Name=CRAWLER_NAME)
        state = resp["Crawler"]["State"]
        elapsed = int(time.time() - start)
        print(f"\r  Status: {state} ({elapsed}s)", end="", flush=True)
        if state == "READY":
            print(f"\n  [OK] Crawler completed")
            break
        if elapsed > 300:
            print(f"\n  [TIMEOUT]")
            break
        time.sleep(10)

    # List gold tables
    print(f"\n[3] Gold tables registered...")
    paginator = glue.get_paginator("get_tables")
    tables = []
    for page in paginator.paginate(DatabaseName=GLUE_DATABASE):
        for t in page["TableList"]:
            if t["Name"].startswith("gold_"):
                tables.append(t["Name"])

    for t in sorted(tables):
        print(f"  ✓ {t}")
    print(f"  Total: {len(tables)} gold_ tables")

    # Setup Athena results location
    print(f"\n[4] Athena results location...")
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="metadata/athena-results/.keep",
            Body=b"# Athena results stored here\n"
        )
        print(f"  [OK] s3://{BUCKET_NAME}/metadata/athena-results/")
    except Exception as e:
        print(f"  [WARN] {e}")

    print(f"\n{'='*60}")
    print(f"  ATHENA READY!")
    print(f"\n  HOW TO USE:")
    print(f"  1. Go to: https://console.aws.amazon.com/athena")
    print(f"  2. Region: ap-south-1")
    print(f"  3. Settings → Manage → Query result location:")
    print(f"     s3://{BUCKET_NAME}/metadata/athena-results/")
    print(f"  4. Database: {GLUE_DATABASE}")
    print(f"  5. Click any gold_ table → Preview table")
    print(f"\n  SAMPLE QUERY:")
    print(f"  SELECT branch_name, total_txn_volume, total_txn_count")
    print(f"  FROM {GLUE_DATABASE}.gold_agg_branch_performance")
    print(f"  ORDER BY total_txn_volume DESC;")
    print(f"\n  NEXT: Install Apache Superset (see README Step 7)")
    print("=" * 60)


if __name__ == "__main__":
    main()
