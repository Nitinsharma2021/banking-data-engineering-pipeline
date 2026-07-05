"""
=============================================================================
PHASE 5 — STEP 2: TRIGGER AND MONITOR ALL 7 GOLD GLUE JOBS
=============================================================================
Purpose  : Start all 7 Gold layer Glue jobs in the correct dependency order
           and monitor until completion
Run      : python step2_run_gold_jobs.py
Expected : All 7 jobs show SUCCEEDED

DEPENDENCY ORDER (strictly enforced):
  1. silver_to_gold_dim_branch         (no deps)
  2. silver_to_gold_dim_customer       (needs dim_branch)
  3. silver_to_gold_dim_account        (needs dim_customer, dim_branch)
  4. silver_to_gold_fact_transactions  (needs all dims)
  5. silver_to_gold_fact_payments      (needs dim_date)
  6. silver_to_gold_fact_credit_risk   (needs dim_customer, dim_date)
  7. silver_to_gold_aggregations       (needs fact_transactions)

BEFORE RUNNING:
  → All 7 Glue Visual ETL jobs must be CREATED in console first
  → step1_create_dim_date.py must have run successfully
=============================================================================
"""

import boto3
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"

GOLD_JOBS = [
    {
        "name":         "silver_to_gold_dim_branch",
        "description":  "dim_branch — 5 branch rows, no dependencies",
        "wait_after":   10,
        "timeout_mins": 15,
        "critical":     True,
    },
    {
        "name":         "silver_to_gold_dim_customer",
        "description":  "dim_customer — 500+ rows, joins dim_branch",
        "wait_after":   10,
        "timeout_mins": 15,
        "critical":     True,
    },
    {
        "name":         "silver_to_gold_dim_account",
        "description":  "dim_account — 1000+ rows, joins dim_customer + dim_branch",
        "wait_after":   10,
        "timeout_mins": 15,
        "critical":     True,
    },
    {
        "name":         "silver_to_gold_fact_transactions",
        "description":  "fact_transactions — 30000+ rows, joins all 3 dims",
        "wait_after":   15,
        "timeout_mins": 25,
        "critical":     True,
    },
    {
        "name":         "silver_to_gold_fact_payments",
        "description":  "fact_payments — 20000 rows, joins dim_date",
        "wait_after":   10,
        "timeout_mins": 20,
        "critical":     False,
    },
    {
        "name":         "silver_to_gold_fact_credit_risk",
        "description":  "fact_credit_risk — 5500 rows, joins dim_customer + dim_date",
        "wait_after":   10,
        "timeout_mins": 15,
        "critical":     False,
    },
    {
        "name":         "silver_to_gold_aggregations",
        "description":  "4 aggregation tables — reads fact_transactions",
        "wait_after":   0,
        "timeout_mins": 25,
        "critical":     False,
    },
]

TERMINAL_STATES = ["SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"]


def run_and_wait(glue, job_name: str, timeout_mins: int) -> tuple:
    """Start a job and wait for terminal state. Returns (status, run_id, exec_time)."""
    try:
        resp   = glue.start_job_run(JobName=job_name)
        run_id = resp["JobRunId"]
    except ClientError as e:
        return "START_FAILED", None, 0

    start = time.time()
    while True:
        elapsed = (time.time() - start) / 60
        if elapsed > timeout_mins:
            return "TIMEOUT", run_id, int(elapsed * 60)

        resp   = glue.get_job_run(JobName=job_name, RunId=run_id)
        run    = resp["JobRun"]
        status = run["JobRunState"]
        mins   = int(elapsed)
        secs   = int((elapsed % 1) * 60)
        print(f"\r      [{mins:02d}:{secs:02d}] {status}...", end="", flush=True)

        if status in TERMINAL_STATES:
            print()
            exec_time = run.get("ExecutionTime", 0)
            error     = run.get("ErrorMessage", "")
            if error and status != "SUCCEEDED":
                print(f"      Error: {error[:200]}")
            return status, run_id, exec_time

        time.sleep(15)


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 5 Step 2: Run All Gold Glue Jobs")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    glue    = boto3.client("glue", region_name=AWS_REGION)
    results = []

    for job in GOLD_JOBS:
        name = job["name"]
        print(f"\n{'─'*65}")
        print(f"  Job : {name}")
        print(f"  Desc: {job['description']}")

        # Check job exists
        try:
            glue.get_job(JobName=name)
        except ClientError:
            print(f"  [SKIP] Job '{name}' not found in Glue!")
            print(f"         Create this job in Glue Visual ETL console first.")
            results.append({"job": name, "status": "NOT_FOUND", "secs": 0})
            if job["critical"]:
                print(f"  [HALT] This is a critical job — stopping here.")
                break
            continue

        print(f"  Starting...")
        status, run_id, secs = run_and_wait(glue, name, job["timeout_mins"])

        sym = "✓" if status == "SUCCEEDED" else "✗"
        print(f"  {sym} {status}  ({secs}s)")
        results.append({"job": name, "status": status, "secs": secs})

        if status != "SUCCEEDED" and job["critical"]:
            print(f"\n  [HALT] Critical job failed. Fix then re-run.")
            break

        if job["wait_after"] > 0 and status == "SUCCEEDED":
            print(f"  Waiting {job['wait_after']}s before next job...")
            time.sleep(job["wait_after"])

    # ── Summary ─────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  GOLD JOB RUN SUMMARY")
    print(f"{'='*65}")
    print(f"\n  {'Job':<45} {'Status':<12} {'Secs':>6}")
    print(f"  {'-'*45} {'-'*12} {'-'*6}")
    ok = 0
    for r in results:
        print(f"  {r['job']:<45} {r['status']:<12} {r['secs']:>6}")
        if r["status"] == "SUCCEEDED":
            ok += 1

    print(f"\n  {ok}/{len(results)} jobs succeeded")
    if ok == len(GOLD_JOBS):
        print(f"\n  ALL GOLD JOBS SUCCEEDED!")
        print(f"  Run: python step3_verify_gold_layer.py")
    else:
        print(f"\n  To debug failures:")
        print(f"  → Glue Console → ETL Jobs → [job] → Runs tab → Error details")
        print(f"  → CloudWatch → Log groups → /aws-glue/jobs/error")
    print("=" * 65)


if __name__ == "__main__":
    main()
