"""
=============================================================================
PHASE 6 — STEP 1: CREATE REDSHIFT SCHEMA + 11 TABLES
=============================================================================
Run: python step1_create_redshift_schema.py
BEFORE RUNNING — update REDSHIFT_HOST below with your actual endpoint
=============================================================================
"""

import psycopg2
import sys


# ─────────────────────────────────────────────────────────────
# UPDATE THIS WITH YOUR ACTUAL ENDPOINT
# ─────────────────────────────────────────────────────────────
REDSHIFT_HOST = "neo-bank-workgroup.843302972838.ap-south-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB   = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASS = "BankAdmin#2025"


DDL_STATEMENTS = [
    ("Create schema", "CREATE SCHEMA IF NOT EXISTS banking;"),

    ("dim_date", """
        DROP TABLE IF EXISTS banking.dim_date CASCADE;
        CREATE TABLE banking.dim_date (
            date_sk         BIGINT      NOT NULL,
            full_date       VARCHAR(10) NOT NULL,
            day_of_month    BIGINT,
            day_of_week     BIGINT,
            day_name        VARCHAR(20),
            week_of_year    INT,
            month_num       INT,
            month_name      VARCHAR(20),
            quarter         INT,
            year            INT,
            is_weekend      INT,
            is_month_end    INT,
            is_month_start  INT,
            fiscal_year     INT,
            fiscal_quarter  VARCHAR(5),
            CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
        )
        DISTSTYLE ALL
        SORTKEY (full_date);
    """),

    ("dim_branch", """
        DROP TABLE IF EXISTS banking.dim_branch CASCADE;
        CREATE TABLE banking.dim_branch (
            branch_sk           INT         NOT NULL,
            branch_code         VARCHAR(10) NOT NULL,
            branch_name         VARCHAR(100),
            city                VARCHAR(100),
            state               VARCHAR(100),
            region              VARCHAR(100),
            source_created_at   VARCHAR(50),
            gold_load_ts        VARCHAR(50),
            gold_layer          VARCHAR(20),
            is_current          INT,
            effective_from      VARCHAR(50),
            effective_to        VARCHAR(50),
            CONSTRAINT pk_dim_branch PRIMARY KEY (branch_sk)
        )
        DISTSTYLE ALL
        SORTKEY (branch_code);
    """),

    ("dim_customer", """
        DROP TABLE IF EXISTS banking.dim_customer CASCADE;
        CREATE TABLE banking.dim_customer (
            customer_sk         INT         NOT NULL,
            customer_id         INT         NOT NULL,
            first_name          VARCHAR(100),
            last_name           VARCHAR(100),
            full_name           VARCHAR(200),
            date_of_birth       VARCHAR(20),
            kyc_status          VARCHAR(50),
            branch_code         VARCHAR(10),
            branch_sk           INT,
            pan_masked          VARCHAR(20),
            email_masked        VARCHAR(150),
            phone_masked        VARCHAR(20),
            source_created_at   VARCHAR(50),
            source_updated_at   VARCHAR(50),
            gold_load_ts        VARCHAR(50),
            gold_layer          VARCHAR(20),
            is_current          INT,
            effective_from      VARCHAR(50),
            effective_to        VARCHAR(50),
            CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
        )
        DISTKEY (customer_sk)
        SORTKEY (customer_id);
    """),

    ("dim_account", """
        DROP TABLE IF EXISTS banking.dim_account CASCADE;
        CREATE TABLE banking.dim_account (
            account_sk          INT         NOT NULL,
            account_id          BIGINT      NOT NULL,
            customer_sk         INT,
            account_type        VARCHAR(50),
            balance             DECIMAL(18,2),
            currency            VARCHAR(10),
            branch_code         VARCHAR(10),
            branch_sk           INT,
            status              VARCHAR(50),
            opened_date         VARCHAR(20),
            source_created_at   VARCHAR(50),
            gold_load_ts        VARCHAR(50),
            gold_layer          VARCHAR(20),
            is_current          INT,
            effective_from      VARCHAR(50),
            effective_to        VARCHAR(50),
            CONSTRAINT pk_dim_account PRIMARY KEY (account_sk)
        )
        DISTKEY (account_sk)
        SORTKEY (account_id);
    """),

    ("fact_transactions", """
        DROP TABLE IF EXISTS banking.fact_transactions CASCADE;
        CREATE TABLE banking.fact_transactions (
            txn_sk          BIGINT NOT NULL,
            txn_id          BIGINT NOT NULL,
            account_sk      INT,
            customer_sk     INT,
            branch_sk       INT,
            date_sk         BIGINT,
            txn_type        VARCHAR(20),
            amount          DECIMAL(18,2),
            debit_amount    DECIMAL(18,2),
            credit_amount   DECIMAL(18,2),
            txn_timestamp   VARCHAR(50),
            txn_date        VARCHAR(10),
            channel         VARCHAR(50),
            status          VARCHAR(50),
            gold_load_ts    VARCHAR(50),
            gold_layer      VARCHAR(20),
            CONSTRAINT pk_fact_transactions PRIMARY KEY (txn_sk)
        )
        DISTKEY (customer_sk)
        SORTKEY (txn_date);
    """),

    ("fact_payments", """
        DROP TABLE IF EXISTS banking.fact_payments CASCADE;
        CREATE TABLE banking.fact_payments (
            payment_sk          BIGINT NOT NULL,
            txn_id              BIGINT NOT NULL,
            date_sk             BIGINT,
            gateway_name        VARCHAR(50),
            gateway_status      VARCHAR(50),
            response_code       VARCHAR(10),
            processing_time_ms  BIGINT,
            device_type         VARCHAR(50),
            geo_location        VARCHAR(100),
            processed_timestamp VARCHAR(50),
            txn_date            VARCHAR(10),
            is_success          INT,
            gold_load_ts        VARCHAR(50),
            gold_layer          VARCHAR(20),
            CONSTRAINT pk_fact_payments PRIMARY KEY (payment_sk)
        )
        DISTKEY (txn_id)
        SORTKEY (txn_date);
    """),

    ("fact_credit_risk", """
        DROP TABLE IF EXISTS banking.fact_credit_risk CASCADE;
        CREATE TABLE banking.fact_credit_risk (
            credit_sk               BIGINT NOT NULL,
            customer_sk             INT,
            customer_id             INT,
            date_sk                 BIGINT,
            credit_score            INT,
            risk_grade              VARCHAR(20),
            risk_band               VARCHAR(20),
            external_active_loans   INT,
            external_overdue_amount DECIMAL(18,2),
            bureau_pull_date        VARCHAR(10),
            gold_load_ts            VARCHAR(50),
            gold_layer              VARCHAR(20),
            CONSTRAINT pk_fact_credit_risk PRIMARY KEY (credit_sk)
        )
        DISTKEY (customer_sk)
        SORTKEY (bureau_pull_date);
    """),

    ("agg_daily_balances", """
        DROP TABLE IF EXISTS banking.agg_daily_balances;
        CREATE TABLE banking.agg_daily_balances (
            account_sk          INT,
            account_id          BIGINT,
            txn_date            VARCHAR(10),
            total_credit        DECIMAL(18,2),
            total_debit         DECIMAL(18,2),
            net_balance_change  DECIMAL(18,2),
            txn_count           BIGINT,
            avg_txn_amount      DECIMAL(18,2),
            gold_load_ts        VARCHAR(50),
            agg_type            VARCHAR(50)
        )
        DISTKEY (account_sk)
        SORTKEY (txn_date);
    """),

    ("agg_monthly_summary", """
        DROP TABLE IF EXISTS banking.agg_monthly_summary;
        CREATE TABLE banking.agg_monthly_summary (
            branch_sk           INT,
            branch_code         VARCHAR(10),
            branch_name         VARCHAR(100),
            year_month          VARCHAR(7),
            total_txn_volume    DECIMAL(18,2),
            total_txn_count     BIGINT,
            total_credits       DECIMAL(18,2),
            total_debits        DECIMAL(18,2),
            avg_txn_amount      DECIMAL(18,2),
            active_accounts     BIGINT,
            gold_load_ts        VARCHAR(50),
            agg_type            VARCHAR(50)
        )
        DISTKEY (branch_sk)
        SORTKEY (year_month);
    """),

    ("agg_branch_performance", """
        DROP TABLE IF EXISTS banking.agg_branch_performance;
        CREATE TABLE banking.agg_branch_performance (
            branch_sk           INT,
            branch_code         VARCHAR(10),
            branch_name         VARCHAR(100),
            city                VARCHAR(100),
            region              VARCHAR(100),
            total_txn_volume    DECIMAL(18,2),
            total_txn_count     BIGINT,
            avg_txn_amount      DECIMAL(18,2),
            max_single_txn      DECIMAL(18,2),
            first_txn_date      VARCHAR(10),
            last_txn_date       VARCHAR(10),
            unique_accounts     BIGINT,
            unique_customers    BIGINT,
            gold_load_ts        VARCHAR(50),
            agg_type            VARCHAR(50)
        )
        DISTSTYLE ALL
        SORTKEY (branch_code);
    """),

    ("agg_customer_360", """
        DROP TABLE IF EXISTS banking.agg_customer_360;
        CREATE TABLE banking.agg_customer_360 (
            customer_sk             INT,
            lifetime_txn_volume     DECIMAL(18,2),
            lifetime_txn_count      BIGINT,
            avg_txn_amount          DECIMAL(18,2),
            total_credits_received  DECIMAL(18,2),
            total_debits_made       DECIMAL(18,2),
            largest_single_txn      DECIMAL(18,2),
            first_txn_date          VARCHAR(10),
            last_txn_date           VARCHAR(10),
            active_days             BIGINT,
            num_accounts            BIGINT,
            gold_load_ts            VARCHAR(50),
            agg_type                VARCHAR(50)
        )
        DISTKEY (customer_sk)
        SORTKEY (customer_sk);
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
    print("  PHASE 6 STEP 1: Create Redshift Schema + 11 Tables")
    print(f"  Host: {REDSHIFT_HOST}")
    print("=" * 60)

    if "ACCOUNT_ID" in REDSHIFT_HOST:
        print("\n  [ERROR] Update REDSHIFT_HOST with your actual endpoint!")
        print("  Get from: Redshift Console → neo-bank-workgroup → Workgroup details")
        sys.exit(1)

    print("\n  Connecting to Redshift...")
    try:
        conn   = get_connection()
        cursor = conn.cursor()
        print("  [OK] Connected")
    except Exception as e:
        print(f"  [FAIL] {e}")
        print("\n  Check:")
        print("  - Security group has port 5439 open for your IP")
        print("  - Workgroup is publicly accessible")
        sys.exit(1)

    for table_name, ddl in DDL_STATEMENTS:
        try:
            cursor.execute(ddl)
            conn.commit()
            print(f"  [OK]  {table_name}")
        except Exception as e:
            conn.rollback()
            print(f"  [FAIL] {table_name}: {e}")

    cursor.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'banking' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tables = [r[0] for r in cursor.fetchall()]

    print(f"\n  Tables created: {len(tables)}")
    for t in tables:
        print(f"    ✓ banking.{t}")
    conn.close()

    print(f"\n{'='*60}")
    if len(tables) == 11:
        print(f"  ALL 11 TABLES CREATED SUCCESSFULLY")
        print(f"\n  NEXT: python step2_load_gold_to_redshift.py")
    else:
        print(f"  Only {len(tables)}/11 tables created")
    print("=" * 60)


if __name__ == "__main__":
    main()
