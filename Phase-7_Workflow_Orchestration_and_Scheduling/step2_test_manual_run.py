"""
=============================================================================
PHASE 7 — STEP 2: TEST MANUAL EXECUTION
=============================================================================
Purpose : Trigger state machine manually to test before scheduling.
          Polls execution status until terminal.
Run     : python step2_test_manual_run.py
=============================================================================
"""

import json
import sys
import time
import uuid
import boto3
from datetime import datetime, timezone


ACCOUNT_ID         = "843302972838"
AWS_REGION         = "ap-south-1"
STATE_MACHINE_NAME = "neo-bank-etl-orchestrator"
STATE_MACHINE_ARN  = f"arn:aws:states:{AWS_REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"

POLL_INTERVAL_SEC  = 30
MAX_WAIT_MINUTES   = 60


def format_duration(seconds):
    minutes = int(seconds // 60)
    secs    = int(seconds % 60)
    return f"{minutes}m {secs}s"


def main():
    print("=" * 60)
    print("  PHASE 7 STEP 2: Test Manual Execution")
    print("=" * 60)

    sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    exec_id  = uuid.uuid4().hex[:8]
    exec_name = f"manual-{today}-{exec_id}"

    input_payload = {
        "run_date":    today,
        "trigger":     "manual",
        "triggered_by": "step2_test_manual_run.py"
    }

    print(f"\n[1] Starting execution: {exec_name}")
    print(f"    State machine: {STATE_MACHINE_NAME}")
    print(f"    Input: {json.dumps(input_payload)}")

    try:
        resp = sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=exec_name,
            input=json.dumps(input_payload)
        )
        exec_arn = resp["executionArn"]
        start_ts = resp["startDate"]
        print(f"    [OK] Started at {start_ts.isoformat()}")
        print(f"    Execution ARN: {exec_arn}")
    except Exception as e:
        print(f"    [FAIL] {e}")
        print(f"\n  Run step1_create_state_machine.py first")
        sys.exit(1)

    console_url = (
        f"https://{AWS_REGION}.console.aws.amazon.com/states/home"
        f"?region={AWS_REGION}#/v2/executions/details/{exec_arn}"
    )
    print(f"\n[2] View live in console:")
    print(f"    {console_url}")

    print(f"\n[3] Polling execution status (every {POLL_INTERVAL_SEC}s)...")
    print(f"    Expected duration: 17-20 minutes for full run")
    print(f"    Max wait: {MAX_WAIT_MINUTES} minutes")
    print()

    start_time = time.time()
    last_state = None

    while True:
        elapsed = int(time.time() - start_time)

        if elapsed > MAX_WAIT_MINUTES * 60:
            print(f"\n  [TIMEOUT] Execution still running after {MAX_WAIT_MINUTES} min")
            print(f"  Open console to monitor: {console_url}")
            sys.exit(1)

        d = sfn.describe_execution(executionArn=exec_arn)
        status = d["status"]

        try:
            history = sfn.get_execution_history(
                executionArn=exec_arn,
                reverseOrder=True,
                maxResults=10
            )
            current_state = "starting..."
            for ev in history["events"]:
                if ev["type"] == "TaskStateEntered":
                    current_state = ev.get("stateEnteredEventDetails", {}).get("name", "?")
                    break
                elif ev["type"] == "ParallelStateEntered":
                    current_state = ev.get("stateEnteredEventDetails", {}).get("name", "?") + " (parallel)"
                    break
        except Exception:
            current_state = "?"

        if current_state != last_state:
            print(f"  [{format_duration(elapsed)}] {status}: {current_state}")
            last_state = current_state

        if status == "SUCCEEDED":
            duration = (d["stopDate"] - d["startDate"]).total_seconds()
            print(f"\n{'='*60}")
            print(f"  EXECUTION SUCCEEDED")
            print(f"  Duration: {format_duration(duration)}")
            print(f"  Started:  {d['startDate'].isoformat()}")
            print(f"  Ended:    {d['stopDate'].isoformat()}")

            output = json.loads(d.get("output", "{}"))
            if "silverResults" in output:
                print(f"\n  Silver jobs completed: {len(output['silverResults'])}")
            if "factResults" in output:
                print(f"  Fact jobs completed:   {len(output['factResults'])}")

            print(f"\n  CHECK SNS:")
            print(f"  You should have received an email notification.")
            print(f"\n  NEXT: python step3_create_eventbridge_schedule.py")
            print("=" * 60)
            return

        elif status in ("FAILED", "TIMED_OUT", "ABORTED"):
            duration = (d["stopDate"] - d["startDate"]).total_seconds()
            print(f"\n{'='*60}")
            print(f"  EXECUTION {status}")
            print(f"  Duration: {format_duration(duration)}")

            print(f"\n  Failed events:")
            history = sfn.get_execution_history(
                executionArn=exec_arn,
                reverseOrder=True,
                maxResults=20
            )
            for ev in history["events"]:
                if "Failed" in ev["type"] or ev["type"] == "ExecutionFailed":
                    details_key = ev["type"][0].lower() + ev["type"][1:] + "EventDetails"
                    details = ev.get(details_key, {})
                    print(f"    Type:  {ev['type']}")
                    if "error" in details:
                        print(f"    Error: {details['error']}")
                    if "cause" in details:
                        print(f"    Cause: {details['cause'][:200]}")
                    print()

            print(f"  DEBUG IN CONSOLE:")
            print(f"  {console_url}")
            print(f"\n  CHECK SNS:")
            print(f"  You should have received a FAILED email notification.")
            print("=" * 60)
            sys.exit(1)

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
