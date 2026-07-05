"""
=============================================================================
PHASE 6 — STEP 4: VERIFY COMPLETE REDSHIFT LAYER
=============================================================================
Run: python step4_verify_redshift_layer.py
=============================================================================
"""

import psycopg2
import sys
from datetime import datetime, timezone


REDSHIFT_HOST = "neo-bank-workgroup.843302972838.ap-south-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB   = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASS = "BankAdmin#2025"

EXPECTED_TABLES = {
    "dim_date": 3600, "dim_branch": 5,
    "dim_customer": 100, "dim_account": 100,
    "fact_transactions": 1000, "fact_payments": 1000,
    "fact_credit_risk": 100,
    "agg_daily_balances": 10, "agg_monthly_summary": 5,
    "agg_branch_performance": 5, "agg_customer_360": 100,
}

EXPECTED_VIEWS = [
    "vw_branch_performance",
    "vw_customer_risk_profile",
    "vw_daily_txn_summary",
    "vw_payment_channel_analysis",
]


def get_connection():
    return psycopg2.connect(
        host=REDSHIFT_HOST, port=REDSHIFT_PORT,
        database=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PASS,
        sslmode='require', connect_timeout=30,
    )


def main():
    print("=" * 60)
    print("  PHASE 6 STEP 4: Redshift Layer Verification")
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    if "ACCOUNT_ID" in REDSHIFT_HOST:
        print("\n  [ERROR] Update REDSHIFT_HOST!")
        sys.exit(1)

    conn = get_connection()
    cursor = conn.cursor()
    print(f"\n  [OK] Connected to Redshift")

    all_passed = True

    # ── Tables ───────────────────────────────────────────────
    print(f"\n[1] Tables")
    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'banking' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    existing = [r[0] for r in cursor.fetchall()]

    for table, min_rows in EXPECTED_TABLES.items():
        if table in existing:
            cursor.execute(f"SELECT COUNT(*) FROM banking.{table};")
            count = cursor.fetchone()[0]
            ok = count >= min_rows
            sym = "✓" if ok else "✗"
            print(f"  {sym} banking.{table:<30} {count:>10,} rows")
            if not ok:
                all_passed = False
        else:
            print(f"  ✗ banking.{table} NOT FOUND")
            all_passed = False

    # ── Views ────────────────────────────────────────────────
    print(f"\n[2] Views")
    cursor.execute("""
        SELECT table_name FROM information_schema.views
        WHERE table_schema = 'banking' ORDER BY table_name;
    """)
    existing_views = [r[0] for r in cursor.fetchall()]
    for view in EXPECTED_VIEWS:
        if view in existing_views:
            print(f"  ✓ banking.{view}")
        else:
            print(f"  ✗ banking.{view} NOT FOUND")
            all_passed = False

    # ── Star schema join test ────────────────────────────────
    print(f"\n[3] Star schema 4-table join test")
    try:
        cursor.execute("""
            SELECT b.branch_name, COUNT(f.txn_id) AS txns, SUM(f.amount) AS volume
            FROM banking.fact_transactions f
            JOIN banking.dim_branch   b ON f.branch_sk   = b.branch_sk
            JOIN banking.dim_customer c ON f.customer_sk = c.customer_sk
            JOIN banking.dim_date     d ON f.date_sk     = d.date_sk
            GROUP BY b.branch_name
            ORDER BY volume DESC LIMIT 5;
        """)
        rows = cursor.fetchall()
        print(f"  ✓ 4-table join works ({len(rows)} branches)")
        for row in rows:
            print(f"    {row[0]:<25} {row[1]:>8,} txns   INR {float(row[2]):>15,.2f}")
    except Exception as e:
        print(f"  ✗ Join failed: {e}")
        all_passed = False

    # ── Sample queries ───────────────────────────────────────
    print(f"\n[4] Sample analytical queries")

    queries = [
        ("Branch performance", """
            SELECT branch_name, total_transactions, total_volume_inr
            FROM banking.vw_branch_performance
            ORDER BY total_volume_inr DESC LIMIT 3;
        """),
        ("Risk distribution", """
            SELECT risk_band, COUNT(*), AVG(credit_score)
            FROM banking.fact_credit_risk
            GROUP BY risk_band ORDER BY AVG(credit_score) DESC;
        """),
        ("KYC status", """
            SELECT kyc_status, COUNT(*) FROM banking.dim_customer
            GROUP BY kyc_status ORDER BY COUNT(*) DESC;
        """),
    ]

    for qname, sql in queries:
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            print(f"  ✓ {qname} ({len(rows)} rows)")
        except Exception as e:
            print(f"  ✗ {qname}: {e}")
            all_passed = False

    conn.close()

    print(f"\n{'='*60}")
    if all_passed:
        print(f"  ALL CHECKS PASSED!")
        print(f"\n  REDSHIFT READY FOR BI:")
        print(f"  Server:   {REDSHIFT_HOST}")
        print(f"  Database: dev")
        print(f"  Schema:   banking")
        print(f"  User:     admin")
        print(f"\n  PHASE 6 NEARLY COMPLETE!")
        print(f"  NEXT: python step5_setup_athena.py")
        print(f"  THEN: install Apache Superset (see README)")
    else:
        print(f"  SOME CHECKS FAILED — see [✗] items above")
    print("=" * 60)


if __name__ == "__main__":
    main()
