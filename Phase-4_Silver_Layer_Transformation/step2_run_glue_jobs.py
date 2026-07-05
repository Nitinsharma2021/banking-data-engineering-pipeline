"""
=============================================================================
PHASE 4 — STEP 2: TRIGGER AND MONITOR ALL 6 GLUE JOBS
=============================================================================
Purpose  : Start all 6 Glue Visual ETL jobs in sequence and monitor
           their status until completion
Run      : python step2_run_glue_jobs.py
Expected : All 6 jobs show SUCCEEDED status

RUN ORDER (dependencies matter):
  1. bronze_to_silver_branches       ← first (no dependencies)
  2. bronze_to_silver_customers      ← after branches (FK: branch_code)
  3. bronze_to_silver_accounts       ← after customers (FK: customer_id)
  4. bronze_to_silver_transactions   ← after accounts (FK: account_id)
  5. bronze_to_silver_payment_gateway← independent
  6. bronze_to_silver_credit_bureau  ← independent

BEFORE RUNNING:
  All 6 Glue Visual ETL jobs must be CREATED in AWS Console first.
  This script only TRIGGERS them — it does not create them.
=============================================================================
"""

import boto3
import time
from datetime import datetime, timezone
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"

# Jobs in run order
JOB_RUN_ORDER = [
    {
        "name":        "bronze_to_silver_branches",
        "description": "Branches — 5 rows, full load, no PII",
        "wait_before":  0,      # seconds to wait before starting
        "timeout_mins": 10,
    },
    {
        "name":        "bronze_to_silver_customers",
        "description": "Customers — 500+ rows, PII masking, 5 DQ rules",
        "wait_before":  5,
        "timeout_mins": 15,
    },
    {
        "name":        "bronze_to_silver_accounts",
        "description": "Accounts — 1000+ rows, 7 DQ rules",
        "wait_before":  5,
        "timeout_mins": 15,
    },
    {
        "name":        "bronze_to_silver_transactions",
        "description": "Transactions — 30000+ rows, 7 DQ rules",
        "wait_before":  5,
        "timeout_mins": 20,
    },
    {
        "name":        "bronze_to_silver_payment_gateway",
        "description": "Payment Gateway — 20000 rows, 7 DQ rules",
        "wait_before":  0,
        "timeout_mins": 15,
    },
    {
        "name":        "bronze_to_silver_credit_bureau",
        "description": "Credit Bureau — 5500 rows, 6 DQ rules",
        "wait_before":  0,
        "timeout_mins": 15,
    },
]

TERMINAL_STATES = ["SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"]


def start_job(glue, job_name: str) -> str:
    """Start a Glue job and return its run ID."""
    resp   = glue.start_job_run(JobName=job_name)
    run_id = resp["JobRunId"]
    return run_id


def get_job_status(glue, job_name: str, run_id: str) -> dict:
    """Get current status and details of a job run."""
    resp = glue.get_job_run(JobName=job_name, RunId=run_id)
    run  = resp["JobRun"]
    return {
        "status":        run["JobRunState"],
        "started_on":    run.get("StartedOn", ""),
        "completed_on":  run.get("CompletedOn", ""),
        "error_message": run.get("ErrorMessage", ""),
        "exec_time":     run.get("ExecutionTime", 0),
    }


def wait_for_job(glue, job_name: str, run_id: str, timeout_mins: int) -> str:
    """Poll job status every 15 seconds until terminal state."""
    print(f"      Waiting for job to complete (timeout: {timeout_mins} min)...")
    start     = time.time()
    poll_secs = 15

    while True:
        elapsed = (time.time() - start) / 60
        if elapsed > timeout_mins:
            print(f"      [TIMEOUT] Job exceeded {timeout_mins} minutes")
            return "TIMEOUT"

        info   = get_job_status(glue, job_name, run_id)
        status = info["status"]
        mins   = int(elapsed)
        secs   = int((elapsed - mins) * 60)
        print(f"      [{mins:02d}:{secs:02d}] Status: {status}", end="\r", flush=True)

        if status in TERMINAL_STATES:
            print()  # newline
            return status

        time.sleep(poll_secs)


def print_job_result(job_name: str, status: str, info: dict):
    """Print final result for one job."""
    icon = "✓" if status == "SUCCEEDED" else "✗"
    print(f"  {icon} {job_name}")
    print(f"    Status  : {status}")
    if info.get("exec_time"):
        print(f"    Duration: {info['exec_time']} seconds")
    if status != "SUCCEEDED" and info.get("error_message"):
        print(f"    Error   : {info['error_message'][:200]}")


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 4 Step 2: Run All Glue Jobs")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 65)

    glue    = boto3.client("glue", region_name=AWS_REGION)
    results = []

    for job_config in JOB_RUN_ORDER:
        job_name = job_config["name"]
        print(f"\n{'─'*65}")
        print(f"  Job : {job_name}")
        print(f"  Desc: {job_config['description']}")

        # Check job exists in Glue
        try:
            glue.get_job(JobName=job_name)
        except ClientError as e:
            if "EntityNotFoundException" in str(e):
                print(f"  [SKIP] Job not found in Glue!")
                print(f"         Create '{job_name}' in Glue Visual ETL console first")
                results.append({"job": job_name, "status": "NOT_FOUND"})
                continue
            raise e

        # Wait before starting (for dependency ordering)
        if job_config["wait_before"] > 0:
            print(f"  Waiting {job_config['wait_before']}s before starting...")
            time.sleep(job_config["wait_before"])

        # Start the job
        print(f"  Starting job...")
        try:
            run_id = start_job(glue, job_name)
            print(f"  Run ID: {run_id}")
        except ClientError as e:
            print(f"  [FAIL] Could not start job: {e}")
            results.append({"job": job_name, "status": "START_FAILED"})
            continue

        # Wait for completion
        final_status = wait_for_job(glue, job_name, run_id, job_config["timeout_mins"])
        final_info   = get_job_status(glue, job_name, run_id)

        print_job_result(job_name, final_status, final_info)
        results.append({
            "job":    job_name,
            "status": final_status,
            "run_id": run_id,
            "secs":   final_info.get("exec_time", 0),
        })

        # Stop if a critical job fails
        critical = ["bronze_to_silver_branches",
                    "bronze_to_silver_customers",
                    "bronze_to_silver_accounts"]
        if final_status != "SUCCEEDED" and job_name in critical:
            print(f"\n  [HALT] Critical job failed: {job_name}")
            print(f"         Fix the error then re-run this script")
            break

    # ── Summary ─────────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  GLUE JOB RUN SUMMARY")
    print(f"{'='*65}")
    print(f"\n  {'Job Name':<40} {'Status':<12} {'Secs':>6}")
    print(f"  {'-'*40} {'-'*12} {'-'*6}")
    ok_count = 0
    for r in results:
        secs = r.get("secs", 0)
        print(f"  {r['job']:<40} {r['status']:<12} {secs:>6}")
        if r["status"] == "SUCCEEDED":
            ok_count += 1

    print(f"\n  {ok_count}/{len(results)} jobs succeeded")

    if ok_count == len(JOB_RUN_ORDER):
        print(f"\n  ALL JOBS SUCCEEDED!")
        print(f"  Silver layer is now populated.")
        print(f"\n  NEXT: Run step3_verify_silver_layer.py")
    else:
        print(f"\n  To debug failed jobs:")
        print(f"  → AWS Console → Glue → ETL Jobs → [job name] → Runs tab")
        print(f"  → Click run → 'Error details' or 'Logs' tab")
        print(f"  → CloudWatch → Log groups → /aws-glue/jobs/error")
    print("=" * 65)


if __name__ == "__main__":
    main()
