"""
=============================================================================
PHASE 5 — VERIFICATION SCRIPT: Verify Gold Layer
=============================================================================
Purpose  : Read all 11 gold/ Parquet files from S3 and verify content
Run      : python step2_verify_gold_layer.py
Expected : All 11 gold tables present with correct row counts
=============================================================================
"""

import boto3
import pandas as pd
import io
from datetime import datetime, timezone


AWS_REGION  = "ap-south-1"
BUCKET_NAME = "neo-bank-datalake"

GOLD_TABLES = {
    "gold/dim_date/":               {"min_rows": 3650, "type": "dim",  "key": "date_sk"},
    "gold/dim_branch/":             {"min_rows": 5,    "type": "dim",  "key": "branch_sk"},
    "gold/dim_customer/":           {"min_rows": 100,  "type": "dim",  "key": "customer_sk"},
    "gold/dim_account/":            {"min_rows": 100,  "type": "dim",  "key": "account_sk"},
    "gold/fact_transactions/":      {"min_rows": 1000, "type": "fact", "key": "txn_sk"},
    "gold/fact_payments/":          {"min_rows": 1000, "type": "fact", "key": "payment_sk"},
    "gold/fact_credit_risk/":       {"min_rows": 100,  "type": "fact", "key": "credit_sk"},
    "gold/agg_daily_balances/":     {"min_rows": 100,  "type": "agg",  "key": None},
    "gold/agg_monthly_summary/":    {"min_rows": 5,    "type": "agg",  "key": None},
    "gold/agg_branch_performance/": {"min_rows": 5,    "type": "agg",  "key": None},
    "gold/agg_customer_360/":       {"min_rows": 100,  "type": "agg",  "key": None},
}


def list_parquet_files(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
    return files


def read_parquet(s3, key):
    buf = io.BytesIO()
    s3.download_fileobj(BUCKET_NAME, key, buf)
    buf.seek(0)
    return pd.read_parquet(buf)


def main():
    print("=" * 60)
    print("  PHASE 5 — Gold Layer Verification")
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    results = []
    total_rows = 0

    for prefix, cfg in GOLD_TABLES.items():
        name = prefix.replace("gold/", "").replace("/", "")
        print(f"\n[{cfg['type'].upper()}] {name}")

        files = list_parquet_files(s3, prefix)
        if not files:
            print(f"  [FAIL] No Parquet files found")
            results.append((name, "FAIL", 0))
            continue

        rows = 0
        size_kb = 0
        for f in files:
            try:
                df = read_parquet(s3, f["Key"])
                rows += len(df)
                size_kb += f["Size"] / 1024

                if cfg["key"] and cfg["key"] in df.columns:
                    null_count = df[cfg["key"]].isna().sum()
                    if null_count == 0:
                        print(f"  [OK]   PK '{cfg['key']}' has no nulls")
                    else:
                        print(f"  [WARN] {null_count} nulls in PK '{cfg['key']}'")

                # Check for FK = 0 (failed joins) in fact tables
                if cfg["type"] == "fact":
                    fk_cols = [c for c in df.columns if c.endswith("_sk") and c != cfg["key"]]
                    for fk in fk_cols:
                        zero_count = (df[fk] == 0).sum()
                        if zero_count > 0:
                            pct = (zero_count / len(df)) * 100
                            print(f"  [WARN] {fk}: {zero_count} rows = 0 ({pct:.1f}% join missed)")

            except Exception as e:
                print(f"  [FAIL] Read error: {e}")
                continue

        if rows >= cfg["min_rows"]:
            print(f"  [PASS] {rows:,} rows ({size_kb:.1f} KB)")
            results.append((name, "PASS", rows))
            total_rows += rows
        else:
            print(f"  [FAIL] {rows:,} rows (expected >= {cfg['min_rows']:,})")
            results.append((name, "FAIL", rows))

    # Summary
    print(f"\n{'='*60}")
    print(f"  VERIFICATION SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Table':<30} {'Status':<8} {'Rows':>10}")
    print(f"  {'-'*30} {'-'*8} {'-'*10}")
    passed = 0
    for name, status, rows in results:
        sym = "✓" if status == "PASS" else "✗"
        print(f"  {sym} {name:<28} {status:<8} {rows:>10,}")
        if status == "PASS":
            passed += 1

    print(f"\n  Tables passed: {passed}/{len(results)}")
    print(f"  Total rows:    {total_rows:,}")

    if passed == len(results):
        print(f"\n  PHASE 5 COMPLETE!")
        print(f"  Gold star schema is fully built:")
        print(f"    4 dimensions + 3 facts + 4 aggregations = 11 tables")
        print(f"\n  READY FOR PHASE 6: Redshift + Power BI")
    else:
        print(f"\n  Some tables failed. Re-run failed Glue jobs.")
    print("=" * 60)


if __name__ == "__main__":
    main()
