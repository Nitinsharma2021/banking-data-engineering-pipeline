"""
=============================================================================
PHASE 6 — STEP 2: LOAD GOLD → REDSHIFT (via Glue Data Catalog / Spectrum)
=============================================================================
Strategy : CREATE EXTERNAL SCHEMA (ext_gold) → Glue DB (noe_bank_db)
           Then INSERT INTO each Redshift table SELECT from ext_gold.<table>
           Explicit CASTs applied where Glue inferred VARCHAR for INT/BIGINT.

All 11 tables:
  dim_date, dim_branch, dim_customer, dim_account,
  fact_transactions, fact_payments, fact_credit_risk,
  agg_daily_balances, agg_monthly_summary,
  agg_branch_performance, agg_customer_360

Run: python step2_load_gold_to_redshift_FINAL.py
=============================================================================
"""

import psycopg2
import sys

# ─────────────────────────────────────────────────────────────
REDSHIFT_HOST = "neo-bank-workgroup.843302972838.ap-south-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB   = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASS = "BankAdmin#2025"

IAM_ROLE_ARN  = "arn:aws:iam::843302972838:role/AmeripriseBankGlueRole"
GLUE_DATABASE = "noe_bank_db"       # actual AWS Glue DB name (noe not neo)
EXT_SCHEMA    = "ext_gold"
AWS_REGION    = "ap-south-1"
# ─────────────────────────────────────────────────────────────

E = EXT_SCHEMA

# (redshift_table, glue_table, min_expected_rows)
LOAD_CONFIGS = [
    ("banking.dim_date",               "dim_date",               3600),
    ("banking.dim_branch",             "dim_branch",                5),
    ("banking.dim_customer",           "dim_customer",            100),
    ("banking.dim_account",            "dim_account",             100),
    ("banking.fact_transactions",      "fact_transactions",      1000),
    ("banking.fact_payments",          "fact_payments",          1000),
    ("banking.fact_credit_risk",       "fact_credit_risk",        100),
    ("banking.agg_daily_balances",     "agg_daily_balances",       10),
    ("banking.agg_monthly_summary",    "agg_monthly_summary",       5),
    ("banking.agg_branch_performance", "agg_branch_performance",    5),
    ("banking.agg_customer_360",       "agg_customer_360",        100),
]


def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        database=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASS,
        sslmode='require', connect_timeout=30,
    )


def create_external_schema(cursor, conn):
    print(f"\n  [SETUP] Creating external schema '{EXT_SCHEMA}' → Glue DB '{GLUE_DATABASE}'")
    try:
        cursor.execute(f"DROP SCHEMA IF EXISTS {EXT_SCHEMA};")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  [WARN] Drop schema: {e}")

    cursor.execute(f"""
        CREATE EXTERNAL SCHEMA {EXT_SCHEMA}
        FROM DATA CATALOG
        DATABASE '{GLUE_DATABASE}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        REGION '{AWS_REGION}'
        CREATE EXTERNAL DATABASE IF NOT EXISTS;
    """)
    conn.commit()
    print(f"  [OK]  External schema '{EXT_SCHEMA}' ready")


def check_glue_table(cursor, glue_table):
    cursor.execute(f"""
        SELECT COUNT(*) FROM svv_external_tables
        WHERE schemaname = '{EXT_SCHEMA}' AND tablename = '{glue_table}';
    """)
    return cursor.fetchone()[0] > 0


def get_insert_sql(redshift_table, glue_table):
    """
    Explicit column-level INSERT with CASTs for tables where Glue
    inferred VARCHAR but Redshift schema expects INT / BIGINT / DECIMAL.
    Falls back to SELECT * for tables with no type mismatch.
    """

    if glue_table == "dim_customer":
        return f"""
            INSERT INTO {redshift_table} (
                customer_sk, customer_id, first_name, last_name, full_name,
                date_of_birth, kyc_status, branch_code, branch_sk,
                pan_masked, email_masked, phone_masked,
                source_created_at, source_updated_at,
                gold_load_ts, gold_layer, is_current, effective_from, effective_to
            )
            SELECT
                CAST(customer_sk  AS INT),
                CAST(customer_id  AS INT),
                first_name, last_name, full_name, date_of_birth, kyc_status, branch_code,
                CAST(branch_sk    AS INT),
                pan_masked, email_masked, phone_masked,
                source_created_at, source_updated_at,
                gold_load_ts, gold_layer,
                CAST(is_current   AS INT),
                effective_from, effective_to
            FROM {E}.dim_customer;
        """

    if glue_table == "dim_account":
        return f"""
            INSERT INTO {redshift_table} (
                account_sk, account_id, customer_sk, account_type,
                balance, currency, branch_code, branch_sk,
                status, opened_date, source_created_at,
                gold_load_ts, gold_layer, is_current, effective_from, effective_to
            )
            SELECT
                CAST(account_sk  AS INT),
                CAST(account_id  AS BIGINT),
                CAST(customer_sk AS INT),
                account_type,
                CAST(balance     AS DECIMAL(18,2)),
                currency, branch_code,
                CAST(branch_sk   AS INT),
                status, opened_date, source_created_at,
                gold_load_ts, gold_layer,
                CAST(is_current  AS INT),
                effective_from, effective_to
            FROM {E}.dim_account;
        """

    if glue_table == "fact_payments":
        return f"""
            INSERT INTO {redshift_table} (
                payment_sk, txn_id, date_sk, gateway_name, gateway_status,
                response_code, processing_time_ms, device_type, geo_location,
                processed_timestamp, txn_date, is_success, gold_load_ts, gold_layer
            )
            SELECT
                CAST(payment_sk         AS BIGINT),
                CAST(txn_id             AS BIGINT),
                CAST(date_sk            AS BIGINT),
                gateway_name, gateway_status,
                LEFT(CAST(response_code AS VARCHAR), 10),
                CAST(processing_time_ms AS BIGINT),
                device_type,
                LEFT(CAST(geo_location  AS VARCHAR), 100),
                processed_timestamp, txn_date,
                CAST(is_success         AS INT),
                gold_load_ts, gold_layer
            FROM {E}.fact_payments;
        """

    if glue_table == "fact_credit_risk":
        return f"""
            INSERT INTO {redshift_table} (
                credit_sk, customer_sk, customer_id, date_sk,
                credit_score, risk_grade, risk_band,
                external_active_loans, external_overdue_amount,
                bureau_pull_date, gold_load_ts, gold_layer
            )
            SELECT
                CAST(credit_sk               AS BIGINT),
                CAST(customer_sk             AS INT),
                CAST(customer_id             AS INT),
                CAST(date_sk                 AS BIGINT),
                CAST(credit_score            AS INT),
                LEFT(CAST(risk_grade        AS VARCHAR), 20),
                LEFT(CAST(risk_band         AS VARCHAR), 20),
                CAST(external_active_loans   AS INT),
                CAST(external_overdue_amount AS DECIMAL(18,2)),
                bureau_pull_date, gold_load_ts, gold_layer
            FROM {E}.fact_credit_risk;
        """

    if glue_table == "agg_daily_balances":
        return f"""
            INSERT INTO {redshift_table} (
                account_sk, txn_date,
                total_credit, total_debit, net_balance_change,
                txn_count, avg_txn_amount, gold_load_ts, agg_type
            )
            SELECT
                account_sk, txn_date,
                total_credit, total_debit, net_balance_change,
                txn_count, avg_txn_amount, gold_load_ts, agg_type
            FROM {E}.agg_daily_balances;
        """

    if glue_table == "agg_monthly_summary":
        return f"""
            INSERT INTO {redshift_table} (
                branch_sk, branch_code, branch_name, year_month,
                total_txn_volume, total_txn_count, total_credits,
                total_debits, avg_txn_amount, active_accounts,
                gold_load_ts, agg_type
            )
            SELECT
                CAST(branch_sk        AS INT),
                branch_code, branch_name, year_month,
                CAST(total_txn_volume AS DECIMAL(18,2)),
                CAST(total_txn_count  AS BIGINT),
                CAST(total_credits    AS DECIMAL(18,2)),
                CAST(total_debits     AS DECIMAL(18,2)),
                CAST(avg_txn_amount   AS DECIMAL(18,2)),
                CAST(active_accounts  AS BIGINT),
                gold_load_ts, agg_type
            FROM {E}.agg_monthly_summary;
        """

    if glue_table == "agg_branch_performance":
        return f"""
            INSERT INTO {redshift_table} (
                branch_sk, branch_code, branch_name, city, region,
                total_txn_volume, total_txn_count, avg_txn_amount,
                max_single_txn, first_txn_date, last_txn_date,
                unique_accounts, unique_customers, gold_load_ts, agg_type
            )
            SELECT
                CAST(branch_sk        AS INT),
                branch_code, branch_name, city, region,
                CAST(total_txn_volume AS DECIMAL(18,2)),
                CAST(total_txn_count  AS BIGINT),
                CAST(avg_txn_amount   AS DECIMAL(18,2)),
                CAST(max_single_txn   AS DECIMAL(18,2)),
                first_txn_date, last_txn_date,
                CAST(unique_accounts  AS BIGINT),
                CAST(unique_customers AS BIGINT),
                gold_load_ts, agg_type
            FROM {E}.agg_branch_performance;
        """

    if glue_table == "agg_customer_360":
        return f"""
            INSERT INTO {redshift_table} (
                customer_sk, lifetime_txn_volume, lifetime_txn_count,
                avg_txn_amount, total_credits_received, total_debits_made,
                largest_single_txn, first_txn_date, last_txn_date,
                active_days, num_accounts, gold_load_ts, agg_type
            )
            SELECT
                CAST(customer_sk            AS INT),
                CAST(lifetime_txn_volume    AS DECIMAL(18,2)),
                CAST(lifetime_txn_count     AS BIGINT),
                CAST(avg_txn_amount         AS DECIMAL(18,2)),
                CAST(total_credits_received AS DECIMAL(18,2)),
                CAST(total_debits_made      AS DECIMAL(18,2)),
                CAST(largest_single_txn     AS DECIMAL(18,2)),
                first_txn_date, last_txn_date,
                CAST(active_days            AS BIGINT),
                CAST(num_accounts           AS BIGINT),
                gold_load_ts, agg_type
            FROM {E}.agg_customer_360;
        """

    # dim_date, dim_branch, fact_transactions — no type mismatches
    return f"INSERT INTO {redshift_table} SELECT * FROM {E}.{glue_table};"


def load_table(cursor, conn, redshift_table, glue_table, min_rows):
    if not check_glue_table(cursor, glue_table):
        print(f"  [SKIP] '{glue_table}' not found in Glue catalog '{GLUE_DATABASE}'")
        return "SKIP", 0

    print(f"  [A] Truncating {redshift_table} ...")
    try:
        cursor.execute(f"TRUNCATE TABLE {redshift_table};")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"      [WARN] {e}")

    print(f"  [B] INSERT from {EXT_SCHEMA}.{glue_table} ...")
    try:
        cursor.execute(get_insert_sql(redshift_table, glue_table))
        conn.commit()
        print(f"      INSERT succeeded")
    except Exception as e:
        conn.rollback()
        print(f"  [FAIL] {e}")
        return "FAIL", 0

    cursor.execute(f"SELECT COUNT(*) FROM {redshift_table};")
    rows = cursor.fetchone()[0]
    ok = rows >= min_rows
    print(f"  [C] Rows: {rows:,}  ({'✓' if ok else 'low — expected >= ' + str(min_rows)})")
    return ("LOADED" if ok else "LOW"), rows


def main():
    print("=" * 60)
    print("  PHASE 6 STEP 2: Load Gold → Redshift via Glue Catalog")
    print(f"  Glue DB    : {GLUE_DATABASE}")
    print(f"  Ext Schema : {EXT_SCHEMA}")
    print(f"  Target     : Redshift dev.banking.*  (11 tables)")
    print("=" * 60)

    try:
        conn   = get_connection()
        cursor = conn.cursor()
        print(f"\n  [OK] Connected to Redshift")
    except Exception as e:
        print(f"\n  [FAIL] Cannot connect: {e}")
        sys.exit(1)

    try:
        create_external_schema(cursor, conn)
    except Exception as e:
        conn.rollback()
        print(f"\n  [FAIL] External schema error: {e}")
        print(f"  Check IAM role has: AWSGlueConsoleFullAccess + S3ReadOnlyAccess")
        conn.close()
        sys.exit(1)

    results = []
    for redshift_table, glue_table, min_rows in LOAD_CONFIGS:
        print(f"\n{'─'*60}")
        print(f"  {redshift_table}  ←  {EXT_SCHEMA}.{glue_table}")
        status, rows = load_table(cursor, conn, redshift_table, glue_table, min_rows)
        results.append((redshift_table, status, rows))

    conn.close()

    print(f"\n{'='*60}")
    print(f"  LOAD SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Table':<35} {'Status':<10} {'Rows':>10}")
    print(f"  {'-'*35} {'-'*10} {'-'*10}")

    ok_count = total_rows = 0
    for table, status, rows in results:
        tname = table.replace("banking.", "")
        print(f"  {tname:<35} {status:<10} {rows:>10,}")
        total_rows += rows
        if status == "LOADED":
            ok_count += 1

    print(f"\n  {ok_count}/{len(results)} tables loaded  |  Total rows: {total_rows:,}")

    if ok_count == len(LOAD_CONFIGS):
        print(f"\n  ALL 11 TABLES LOADED SUCCESSFULLY!")
        print(f"  NEXT: python step3_create_analytical_views.py")
    else:
        print(f"\n  Partial load — check FAIL/SKIP tables above.")
    print("=" * 60)


if __name__ == "__main__":
    main()
