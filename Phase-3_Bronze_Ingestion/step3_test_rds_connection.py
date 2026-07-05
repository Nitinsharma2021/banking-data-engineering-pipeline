"""
=============================================================================
PHASE 3 — STEP 3: TEST RDS SQL SERVER CONNECTION
=============================================================================
Purpose  : Verify Python can connect to your AWS RDS SQL Server instance
           Tests connection, lists schemas, and checks banking tables exist
Run      : python step3_test_rds_connection.py
Expected : All connection checks PASS
=============================================================================
BEFORE RUNNING:
  1. Your RDS instance status must be "Available" in AWS Console
  2. Security group must have port 1433 open for your IP
  3. ODBC Driver 18 must be installed (step2)
  4. Update RDS_ENDPOINT below with your actual endpoint
=============================================================================
"""

import pyodbc
import sys
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — UPDATE RDS_ENDPOINT WITH YOUR ACTUAL ENDPOINT
# ─────────────────────────────────────────────────────────────
RDS_ENDPOINT = "ap-bank-sqlserver.cxis6seisyzq.ap-south-1.rds.amazonaws.com"  # ← CHANGE THIS
RDS_PORT     = 1433
RDS_USER     = "admin"
RDS_PASSWORD = "BankAdmin#2025"
RDS_DATABASE = "ap_bank_db"
DRIVER       = "ODBC Driver 18 for SQL Server"


def build_connection_string():
    return (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={RDS_ENDPOINT},{RDS_PORT};"
        f"DATABASE={RDS_DATABASE};"
        f"UID={RDS_USER};"
        f"PWD={RDS_PASSWORD};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=30;"
    )


def check(label, passed, detail=""):
    status = "[PASS]" if passed else "[FAIL]"
    print(f"  {status}  {label}")
    if detail:
        for line in detail.strip().split("\n"):
            print(f"           {line}")
    return passed


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 3: RDS Connection Test")
    print(f"  Run at: {datetime.now().isoformat()}")
    print("=" * 65)

    if "YOUR-RDS-ENDPOINT" in RDS_ENDPOINT:
        print("\n  [ERROR] You forgot to update RDS_ENDPOINT!")
        print("          Open this script and replace YOUR-RDS-ENDPOINT")
        print("          with your actual RDS endpoint from AWS Console")
        print("          Example: ameriprise-bank-sqlserver.abc123.ap-south-1.rds.amazonaws.com")
        sys.exit(1)

    all_ok = True
    conn   = None

    # ── Check 1: ODBC Driver installed ───────────────────────
    print("\n[1] Checking ODBC Driver...")
    drivers = pyodbc.drivers()
    if DRIVER in drivers:
        check("ODBC Driver 18 for SQL Server found", True, f"Available drivers: {drivers}")
    else:
        check("ODBC Driver 18 for SQL Server found", False,
              f"Found drivers: {drivers}\nRun step2 to install ODBC Driver 18")
        all_ok = False

    # ── Check 2: TCP Connection to RDS ───────────────────────
    print("\n[2] Testing TCP connection to RDS...")
    import socket
    try:
        sock = socket.create_connection((RDS_ENDPOINT, RDS_PORT), timeout=10)
        sock.close()
        check(f"TCP port 1433 reachable at {RDS_ENDPOINT}", True)
    except Exception as e:
        check(f"TCP port 1433 reachable", False,
              f"Error: {e}\n"
              f"Fix: Check security group has port 1433 open for YOUR IP\n"
              f"     Check: https://whatismyip.com  then update security group")
        all_ok = False
        print("\n  Cannot proceed without network connection. Fix security group first.")
        return

    # ── Check 3: SQL Server authentication ───────────────────
    print("\n[3] Testing SQL Server authentication...")
    try:
        conn_str = build_connection_string()
        conn     = pyodbc.connect(conn_str, timeout=30)
        cursor   = conn.cursor()
        cursor.execute("SELECT @@VERSION AS version, GETDATE() AS server_time, @@SERVERNAME AS server_name")
        row = cursor.fetchone()
        check("SQL Server authentication successful", True,
              f"Server time : {row.server_time}\n"
              f"Server name : {row.server_name}\n"
              f"Version     : {str(row.version)[:80]}...")
    except pyodbc.Error as e:
        check("SQL Server authentication", False, f"Error: {e}")
        all_ok = False
        return

    # ── Check 4: Banking schema exists ───────────────────────
    print("\n[4] Checking banking schema exists...")
    cursor.execute("SELECT COUNT(*) FROM sys.schemas WHERE name = 'banking'")
    count = cursor.fetchone()[0]
    if count > 0:
        check("banking schema exists", True)
    else:
        check("banking schema exists", False,
              "Schema not found!\n"
              "Run 01_Create_Tables.sql in DBeaver first")
        all_ok = False

    # ── Check 5: All 4 tables exist ──────────────────────────
    print("\n[5] Checking all 4 banking tables exist...")
    expected_tables = ["branches", "customers", "accounts", "transactions"]
    for table in expected_tables:
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'banking' AND TABLE_NAME = '{table}'
        """)
        exists = cursor.fetchone()[0] > 0
        ok = check(f"banking.{table}", exists)
        all_ok = all_ok and ok

    # ── Check 6: Row counts ───────────────────────────────────
    print("\n[6] Checking row counts (data loaded)...")
    count_checks = {
        "banking.branches":     ("branch_code", 5,     "Should be 5 after incremental script"),
        "banking.customers":    ("customer_id",  100,   "Should be 500+ after historical load"),
        "banking.accounts":     ("account_id",   100,   "Should be 1000+ after historical load"),
        "banking.transactions": ("txn_id",       1000,  "Should be 30000+ after historical load"),
    }
    for table, (pk, min_expected, note) in count_checks.items():
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            actual = cursor.fetchone()[0]
            ok     = actual >= min_expected
            check(
                f"{table}: {actual:,} rows",
                ok,
                note if not ok else f"Good — {actual:,} rows confirmed"
            )
            all_ok = all_ok and ok
        except Exception as e:
            check(f"{table} row count", False, str(e))
            all_ok = False

    # ── Check 7: Verify branch data ──────────────────────────
    print("\n[7] Verifying branch data (BR001-BR005)...")
    cursor.execute("SELECT branch_code, branch_name, city, state FROM banking.branches ORDER BY branch_code")
    rows = cursor.fetchall()
    print(f"\n  {'Branch Code':<12} {'Branch Name':<25} {'City':<15} {'State'}")
    print(f"  {'-'*12} {'-'*25} {'-'*15} {'-'*15}")
    for row in rows:
        print(f"  {row[0]:<12} {row[1]:<25} {row[2]:<15} {row[3]}")

    br_codes = [r[0] for r in rows]
    has_br005 = "BR005" in br_codes
    check("\nBR005 Chennai Hub present (from incremental script)", has_br005,
          "If missing: run 03_Incrementat_data.sql in DBeaver")
    all_ok = all_ok and has_br005

    # ── Check 8: Sample customer data ────────────────────────
    print("\n[8] Sample customer records...")
    cursor.execute("""
        SELECT TOP 5
            c.customer_id, c.first_name, c.last_name,
            c.kyc_status, c.branch_code,
            b.city
        FROM banking.customers c
        JOIN banking.branches b ON c.branch_code = b.branch_code
        ORDER BY c.customer_id
    """)
    rows = cursor.fetchall()
    print(f"\n  {'ID':<8} {'Name':<25} {'KYC Status':<12} {'Branch':<8} {'City'}")
    print(f"  {'-'*8} {'-'*25} {'-'*12} {'-'*8} {'-'*12}")
    for row in rows:
        name = f"{row[1]} {row[2]}"
        print(f"  {row[0]:<8} {name:<25} {row[3]:<12} {row[4]:<8} {row[5]}")

    # ── Check 9: Sample transaction data ─────────────────────
    print("\n[9] Transaction channel breakdown...")
    cursor.execute("""
        SELECT channel, COUNT(*) as txn_count,
               SUM(amount) as total_amount
        FROM banking.transactions
        GROUP BY channel
        ORDER BY txn_count DESC
    """)
    rows = cursor.fetchall()
    print(f"\n  {'Channel':<15} {'Txn Count':>12} {'Total Amount (INR)':>20}")
    print(f"  {'-'*15} {'-'*12} {'-'*20}")
    for row in rows:
        ch  = str(row[0]) if row[0] else "NULL"
        cnt = row[1]
        amt = float(row[2]) if row[2] else 0
        print(f"  {ch:<15} {cnt:>12,} {amt:>20,.2f}")

    # ── Check 10: KYC status breakdown ───────────────────────
    print("\n[10] Customer KYC status breakdown...")
    cursor.execute("""
        SELECT kyc_status, COUNT(*) as count
        FROM banking.customers
        GROUP BY kyc_status
        ORDER BY count DESC
    """)
    rows = cursor.fetchall()
    print(f"\n  {'KYC Status':<12} {'Count':>8}")
    print(f"  {'-'*12} {'-'*8}")
    for row in rows:
        print(f"  {row[0]:<12} {row[1]:>8,}")

    # ── Cleanup ───────────────────────────────────────────────
    if conn:
        conn.close()

    # ── Final summary ─────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  CONNECTION TEST SUMMARY")
    print(f"{'='*65}")
    if all_ok:
        print(f"  ALL CHECKS PASSED!")
        print(f"  RDS SQL Server is correctly set up with banking data.")
        print(f"\n  NEXT STEP: Run step4_extract_rds_to_s3.py")
    else:
        print(f"  SOME CHECKS FAILED")
        print(f"  Fix the [FAIL] items above then re-run this script")
    print("=" * 65)


if __name__ == "__main__":
    main()
