"""
=============================================================================
PHASE 6 — STEP 3: CREATE 4 ANALYTICAL VIEWS
=============================================================================
Run: python step3_create_analytical_views.py
BEFORE RUNNING — update REDSHIFT_HOST below
=============================================================================
"""

import psycopg2
import sys


REDSHIFT_HOST = "neo-bank-workgroup.843302972838.ap-south-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB   = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASS = "BankAdmin#2025"


VIEWS = [
    ("vw_branch_performance", """
        CREATE OR REPLACE VIEW banking.vw_branch_performance AS
        SELECT
            b.branch_code,
            b.branch_name,
            b.city,
            b.state,
            b.region,
            COUNT(f.txn_id)                AS total_transactions,
            SUM(f.amount)                  AS total_volume_inr,
            AVG(f.amount)                  AS avg_txn_amount_inr,
            SUM(f.credit_amount)           AS total_credits_inr,
            SUM(f.debit_amount)            AS total_debits_inr,
            COUNT(DISTINCT f.customer_sk)  AS unique_customers,
            COUNT(DISTINCT f.account_sk)   AS unique_accounts,
            MIN(f.txn_date)                AS first_txn_date,
            MAX(f.txn_date)                AS last_txn_date,
            COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS successful_txns,
            COUNT(CASE WHEN f.status = 'FAILED'  THEN 1 END) AS failed_txns
        FROM banking.fact_transactions f
        JOIN banking.dim_branch b ON f.branch_sk = b.branch_sk
        GROUP BY b.branch_code, b.branch_name, b.city, b.state, b.region;
    """),

    ("vw_customer_risk_profile", """
        CREATE OR REPLACE VIEW banking.vw_customer_risk_profile AS
        SELECT
            c.customer_id,
            c.first_name,
            c.last_name,
            c.kyc_status,
            b.branch_name,
            b.city,
            cr.credit_score,
            cr.risk_grade,
            cr.risk_band,
            cr.external_active_loans,
            cr.external_overdue_amount,
            cr.bureau_pull_date,
            a360.lifetime_txn_volume,
            a360.lifetime_txn_count,
            a360.num_accounts,
            a360.last_txn_date,
            CASE
                WHEN cr.credit_score >= 750 THEN 'LOW RISK'
                WHEN cr.credit_score >= 650 THEN 'MEDIUM RISK'
                WHEN cr.credit_score >= 550 THEN 'HIGH RISK'
                ELSE 'VERY HIGH RISK'
            END AS bank_risk_category
        FROM banking.dim_customer c
        LEFT JOIN banking.dim_branch b ON c.branch_sk = b.branch_sk
        LEFT JOIN banking.fact_credit_risk cr ON c.customer_sk = cr.customer_sk
        LEFT JOIN banking.agg_customer_360 a360 ON c.customer_sk = a360.customer_sk;
    """),

    ("vw_daily_txn_summary", """
        CREATE OR REPLACE VIEW banking.vw_daily_txn_summary AS
        SELECT
            d.full_date,
            d.day_name,
            d.month_name,
            d.year,
            d.quarter,
            d.is_weekend,
            d.fiscal_quarter,
            b.branch_name,
            b.region,
            f.txn_type,
            f.channel,
            COUNT(f.txn_id)        AS txn_count,
            SUM(f.amount)          AS total_amount_inr,
            AVG(f.amount)          AS avg_amount_inr,
            MIN(f.amount)          AS min_amount_inr,
            MAX(f.amount)          AS max_amount_inr,
            COUNT(CASE WHEN f.status = 'SUCCESS' THEN 1 END) AS success_count,
            COUNT(CASE WHEN f.status = 'FAILED'  THEN 1 END) AS fail_count
        FROM banking.fact_transactions f
        JOIN banking.dim_date d   ON f.date_sk   = d.date_sk
        JOIN banking.dim_branch b ON f.branch_sk = b.branch_sk
        GROUP BY d.full_date, d.day_name, d.month_name, d.year,
                 d.quarter, d.is_weekend, d.fiscal_quarter,
                 b.branch_name, b.region, f.txn_type, f.channel;
    """),

    ("vw_payment_channel_analysis", """
        CREATE OR REPLACE VIEW banking.vw_payment_channel_analysis AS
        SELECT
            d.full_date,
            d.month_name,
            d.year,
            p.gateway_name,
            p.device_type,
            p.geo_location,
            COUNT(p.payment_sk)                              AS total_payments,
            SUM(p.is_success)                                AS successful_payments,
            COUNT(p.payment_sk) - SUM(p.is_success)         AS failed_payments,
            ROUND(SUM(p.is_success) * 100.0 / COUNT(p.payment_sk), 2) AS success_rate_pct,
            AVG(p.processing_time_ms)                        AS avg_processing_ms,
            MIN(p.processing_time_ms)                        AS min_processing_ms,
            MAX(p.processing_time_ms)                        AS max_processing_ms
        FROM banking.fact_payments p
        JOIN banking.dim_date d ON p.date_sk = d.date_sk
        GROUP BY d.full_date, d.month_name, d.year,
                 p.gateway_name, p.device_type, p.geo_location;
    """),
]


def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        database=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASS,
        sslmode='require', connect_timeout=30,
    )


def main():
    print("=" * 60)
    print("  PHASE 6 STEP 3: Create Analytical Views")
    print("=" * 60)

    if "ACCOUNT_ID" in REDSHIFT_HOST:
        print("\n  [ERROR] Update REDSHIFT_HOST!")
        sys.exit(1)

    conn = get_connection()
    cursor = conn.cursor()
    print(f"\n  [OK] Connected to Redshift")

    for view_name, ddl in VIEWS:
        try:
            cursor.execute(ddl)
            conn.commit()
            print(f"  [OK]  banking.{view_name}")
        except Exception as e:
            conn.rollback()
            print(f"  [FAIL] {view_name}: {e}")

    print(f"\n  Testing views...")
    for view_name, _ in VIEWS:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM banking.{view_name};")
            count = cursor.fetchone()[0]
            print(f"  [OK]  banking.{view_name}: {count:,} rows")
        except Exception as e:
            print(f"  [WARN] {view_name} test failed: {e}")

    conn.close()

    print(f"\n{'='*60}")
    print(f"  4 VIEWS CREATED — Ready for Superset/BI tools")
    print(f"  NEXT: python step4_verify_redshift_layer.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
