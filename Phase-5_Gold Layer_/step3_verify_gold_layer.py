"""
=============================================================================
PHASE 5 — STEP 3: VERIFY COMPLETE GOLD LAYER
=============================================================================
Purpose  : Full audit of gold/ zone — reads every Parquet file,
           checks row counts, verifies surrogate keys, checks joins worked
Run      : python step3_verify_gold_layer.py
Expected : All 11 gold tables present with correct content
=============================================================================
"""

import boto3
import pandas as pd
import io
import json
from datetime import datetime, timezone
from botocore.exceptions import ClientError


AWS_REGION  = "ap-south-1"
BUCKET_NAME = "ameriprise-bank-datalake"

GOLD_TABLES = {
    # Dimensions
    "gold/dim_date/":        {"min_rows": 3650, "type": "dimension", "pk": "date_sk",    "check_cols": ["full_date","year","month_num","quarter","is_weekend","fiscal_quarter"]},
    "gold/dim_branch/":      {"min_rows": 5,    "type": "dimension", "pk": "branch_sk",  "check_cols": ["branch_code","branch_name","city","state","region","is_current"]},
    "gold/dim_customer/":    {"min_rows": 100,  "type": "dimension", "pk": "customer_sk","check_cols": ["customer_id","full_name","kyc_status","branch_sk","pan_masked","email_masked"]},
    "gold/dim_account/":     {"min_rows": 100,  "type": "dimension", "pk": "account_sk", "check_cols": ["account_id","customer_sk","account_type","balance","status"]},
    # Facts
    "gold/fact_transactions/":{"min_rows": 1000, "type": "fact",     "pk": "txn_sk",     "check_cols": ["txn_id","account_sk","customer_sk","branch_sk","date_sk","amount","debit_amount","credit_amount"]},
    "gold/fact_payments/":   {"min_rows": 1000,  "type": "fact",     "pk": "payment_sk", "check_cols": ["txn_id","date_sk","gateway_name","gateway_status","device_type","is_success"]},
    "gold/fact_credit_risk/":{"min_rows": 100,   "type": "fact",     "pk": "credit_sk",  "check_cols": ["customer_sk","date_sk","credit_score","risk_grade","risk_band","external_overdue_amount"]},
    # Aggregations
    "gold/agg_daily_balances/":     {"min_rows": 100, "type": "aggregation", "pk": None, "check_cols": ["account_sk","txn_date","total_credit","total_debit","net_balance_change","txn_count"]},
    "gold/agg_monthly_summary/":    {"min_rows": 10,  "type": "aggregation", "pk": None, "check_cols": ["branch_sk","year_month","total_txn_volume","total_txn_count","active_accounts"]},
    "gold/agg_branch_performance/": {"min_rows": 5,   "type": "aggregation", "pk": None, "check_cols": ["branch_sk","total_txn_volume","total_txn_count","unique_accounts","unique_customers"]},
    "gold/agg_customer_360/":       {"min_rows": 100, "type": "aggregation", "pk": None, "check_cols": ["customer_sk","lifetime_txn_volume","lifetime_txn_count","num_accounts"]},
}

GOLD_META_COLS = ["gold_load_ts", "gold_layer"]


def ok(msg):   print(f"    [PASS] {msg}")
def fail(msg): print(f"    [FAIL] {msg}")
def info(msg): print(f"           {msg}")


def list_parquet_files(s3, bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    files = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                files.append(obj)
    return files


def read_parquet_s3(s3, bucket, key):
    buf = io.BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pd.read_parquet(buf)


def verify_gold_table(s3, bucket, prefix, config):
    files = list_parquet_files(s3, bucket, prefix)
    if not files:
        fail(f"No Parquet files in {prefix}")
        info(f"Fix: Run the corresponding Gold Glue job")
        return False, 0

    total_rows   = 0
    all_ok_local = True

    for f in files:
        try:
            df   = read_parquet_s3(s3, bucket, f["Key"])
            rows = len(df)
            total_rows += rows
            cols = list(df.columns)

            info(f"File: {f['Key'].split('/')[-1]}  ({rows:,} rows, {len(cols)} cols)")

            # Check Gold metadata columns
            for mc in GOLD_META_COLS:
                if mc not in cols:
                    info(f"  [WARN] Missing gold metadata col: {mc}")
                    all_ok_local = False

            # Check business columns
            missing_cols = [c for c in config["check_cols"] if c not in cols]
            if not missing_cols:
                info(f"  Business columns: all {len(config['check_cols'])} present ✓")
            else:
                info(f"  [WARN] Missing columns: {missing_cols}")
                all_ok_local = False

            # Check surrogate key exists and has no nulls
            pk = config.get("pk")
            if pk and pk in cols:
                null_count = df[pk].isna().sum()
                if null_count == 0:
                    info(f"  Surrogate key '{pk}': no nulls ✓  (range: {df[pk].min()} - {df[pk].max()})")
                else:
                    info(f"  [WARN] {null_count} nulls in '{pk}'")
                    all_ok_local = False

            # Extra checks per table type
            if config["type"] == "dimension":
                if "is_current" in cols:
                    active = df["is_current"].sum()
                    info(f"  is_current=1: {active:,} rows (SCD2 ready)")

            if config["type"] == "fact":
                # Check no FK = 0 (0 means join failed)
                fk_cols = [c for c in cols if c.endswith("_sk") and c != config.get("pk")]
                for fk in fk_cols:
                    zero_count = (df[fk] == 0).sum()
                    if zero_count > 0:
                        pct = (zero_count / len(df)) * 100
                        info(f"  [WARN] {fk}: {zero_count} rows with 0 ({pct:.1f}% — join missed)")
                    else:
                        info(f"  FK {fk}: all rows joined correctly ✓")

            # Special: dim_date range check
            if "dim_date" in prefix and "full_date" in cols:
                min_d = df["full_date"].min()
                max_d = df["full_date"].max()
                info(f"  Date range: {min_d} to {max_d}")

            # Special: fact_transactions debit/credit check
            if "fact_transactions" in prefix:
                if "debit_amount" in cols and "credit_amount" in cols:
                    total_debit  = df["debit_amount"].sum()
                    total_credit = df["credit_amount"].sum()
                    info(f"  Total debits  : INR {total_debit:,.2f}")
                    info(f"  Total credits : INR {total_credit:,.2f}")

            # Special: credit_risk risk_band distribution
            if "fact_credit_risk" in prefix and "risk_band" in cols:
                bands = df["risk_band"].value_counts().to_dict()
                info(f"  Risk bands    : {bands}")

            # Special: branch performance
            if "agg_branch_performance" in prefix and "branch_name" in cols:
                if "total_txn_volume" in cols:
                    top = df.nlargest(3, "total_txn_volume")[["branch_name","total_txn_volume","total_txn_count"]]
                    info(f"  Top branches  :")
                    for _, row in top.iterrows():
                        info(f"    {row['branch_name']}: INR {row['total_txn_volume']:,.2f} ({row['total_txn_count']:,} txns)")

        except Exception as e:
            fail(f"Could not read {f['Key']}: {e}")
            all_ok_local = False

    if total_rows >= config["min_rows"]:
        ok(f"{prefix.replace('gold/','').replace('/','')}  →  {total_rows:,} rows  ✓")
    else:
        fail(f"{prefix.replace('gold/','').replace('/','')}  →  {total_rows:,} rows (expected >= {config['min_rows']:,})")
        all_ok_local = False

    return all_ok_local, total_rows


def print_star_schema_summary(s3, bucket, results_map):
    """Print a readable star schema summary table."""
    print(f"\n{'='*65}")
    print(f"  GOLD LAYER — STAR SCHEMA SUMMARY")
    print(f"{'='*65}")
    print(f"\n  {'Table':<35} {'Type':<12} {'Rows':>10}")
    print(f"  {'-'*35} {'-'*12} {'-'*10}")

    types    = ["dimension", "fact", "aggregation"]
    type_map = {"dimension":"DIMENSION","fact":"FACT","aggregation":"AGGREGATION"}

    for ttype in types:
        for prefix, (ok_flag, rows) in results_map.items():
            cfg = GOLD_TABLES[prefix]
            if cfg["type"] == ttype:
                name   = prefix.replace("gold/","").replace("/","")
                tname  = type_map[ttype]
                sym    = "✓" if ok_flag else "✗"
                print(f"  {sym} {name:<33} {tname:<12} {rows:>10,}")


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 5 Step 3: Gold Layer Verification")
    print(f"  Run at: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    s3          = boto3.client("s3", region_name=AWS_REGION)
    all_results = {}
    passed_count= 0

    for prefix, config in GOLD_TABLES.items():
        ttype = config["type"].upper()
        print(f"\n  [{ttype}] {prefix}")
        ok_flag, rows = verify_gold_table(s3, BUCKET_NAME, prefix, config)
        all_results[prefix] = (ok_flag, rows)
        if ok_flag:
            passed_count += 1

    print_star_schema_summary(s3, BUCKET_NAME, all_results)

    total = len(GOLD_TABLES)
    print(f"\n  {passed_count}/{total} gold tables verified")

    if passed_count == total:
        print(f"\n  PHASE 5 COMPLETE!")
        print(f"  Gold Star Schema is fully built and verified.")
        print(f"\n  WHAT YOU CAN DO NOW:")
        print(f"  1. Query with Athena: SELECT * FROM gold_fact_transactions LIMIT 10")
        print(f"  2. Load to Redshift (Phase 6) for Power BI dashboards")
        print(f"  3. Connect Glue Crawler on gold/ to register all tables")
        print(f"\n  COPY THIS FOR PHASE 6 (paste to new chat):")
        print(f"""
  I completed Phase 5 of the Ameriprise Bank AWS DE project.
  Gold Star Schema is built with 4 dims + 3 facts + 4 aggs.
  Bucket: ameriprise-bank-datalake  Region: ap-south-1  OS: Ubuntu
  Give me Phase 6: Redshift Serverless + Power BI. Same depth.
        """)
    else:
        print(f"\n  Fix [FAIL] items then re-run this verification.")
        print(f"  Check Glue job logs: CloudWatch → /aws-glue/jobs/error")
    print("=" * 65)


if __name__ == "__main__":
    main()
