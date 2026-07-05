"""
=============================================================================
PHASE 7 — STEP 1: CREATE STATE MACHINE
=============================================================================
Purpose : Reads phase7_state_machine.asl.json, substitutes ACCOUNT_ID,
          and creates the Step Functions state machine.
Run     : python step1_create_state_machine.py
=============================================================================
"""

import json
import sys
import boto3
from pathlib import Path


# ─────────────────────────────────────────────────────────────
# UPDATE THESE 3 VALUES BEFORE RUNNING
# ─────────────────────────────────────────────────────────────
ACCOUNT_ID      = "843302972838"
SNS_TOPIC_ARN   = f"arn:aws:sns:us-east-1:843302972838:neo-bank-etl-alerts"
STEPFN_ROLE_ARN = f"arn:aws:iam::843302972838:role/AmerispriseBankStepFunctionsRole"
# ─────────────────────────────────────────────────────────────

AWS_REGION         = "ap-south-1"
STATE_MACHINE_NAME = "neo-bank-etl-orchestrator"
LOG_GROUP_NAME     = "/aws/vendedlogs/states/neo-bank-etl"
ASL_FILE           = "/home/shreyansh-jain/Documents/Ameriprise_bank_project/phase7_complete_package/phase7_final/phase7_state_machine.asl.json"


def main():
    print("=" * 60)
    print("  PHASE 7 STEP 1: Create Step Functions State Machine")
    print("=" * 60)

    if ACCOUNT_ID == "843302972838":
        print("\n  [WARN] Verify ACCOUNT_ID is correct for your AWS account")

    asl_path = Path(ASL_FILE)
    if not asl_path.exists():
        print(f"\n  [ERROR] ASL file not found: {ASL_FILE}")
        print("  Place phase7_state_machine.asl.json in same folder as this script")
        sys.exit(1)

    print(f"\n[1] Reading ASL definition from {ASL_FILE}...")
    asl_text = asl_path.read_text()

    print(f"[2] Substituting ACCOUNT_ID...")
    asl_text = asl_text.replace("ACCOUNT_ID", ACCOUNT_ID)

    try:
        json.loads(asl_text)
        print("    [OK] ASL is valid JSON")
    except json.JSONDecodeError as e:
        print(f"    [FAIL] ASL is invalid JSON: {e}")
        sys.exit(1)

    print(f"\n[3] Creating CloudWatch Log Group...")
    logs = boto3.client("logs", region_name=AWS_REGION)
    try:
        logs.create_log_group(logGroupName=LOG_GROUP_NAME)
        print(f"    [CREATED] {LOG_GROUP_NAME}")
    except logs.exceptions.ResourceAlreadyExistsException:
        print(f"    [EXISTS]  {LOG_GROUP_NAME}")

    log_group_arn = f"arn:aws:logs:{AWS_REGION}:{ACCOUNT_ID}:log-group:{LOG_GROUP_NAME}:*"

    print(f"\n[4] Creating state machine: {STATE_MACHINE_NAME}...")
    sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

    try:
        existing = sfn.list_state_machines()
        for sm in existing["stateMachines"]:
            if sm["name"] == STATE_MACHINE_NAME:
                print(f"    [EXISTS] {sm['stateMachineArn']}")
                print(f"\n    Updating definition...")
                sfn.update_state_machine(
                    stateMachineArn=sm["stateMachineArn"],
                    definition=asl_text,
                    roleArn=STEPFN_ROLE_ARN,
                    loggingConfiguration={
                        "level": "ALL",
                        "includeExecutionData": True,
                        "destinations": [{
                            "cloudWatchLogsLogGroup": {"logGroupArn": log_group_arn}
                        }]
                    },
                    tracingConfiguration={"enabled": True},
                )
                print(f"    [UPDATED] State machine definition")
                print(f"\n{'='*60}")
                print(f"  STATE MACHINE READY")
                print(f"  ARN: {sm['stateMachineArn']}")
                print(f"\n  NEXT: python step2_test_manual_run.py")
                print("=" * 60)
                return

        resp = sfn.create_state_machine(
            name=STATE_MACHINE_NAME,
            definition=asl_text,
            roleArn=STEPFN_ROLE_ARN,
            type="STANDARD",
            loggingConfiguration={
                "level": "ALL",
                "includeExecutionData": True,
                "destinations": [{
                    "cloudWatchLogsLogGroup": {"logGroupArn": log_group_arn}
                }]
            },
            tracingConfiguration={"enabled": True},
            tags=[
                {"key": "project", "value": "ameriprise-bank-de-pipeline"},
                {"key": "phase",   "value": "7"},
                {"key": "env",     "value": "dev"},
            ]
        )
        sm_arn = resp["stateMachineArn"]
        print(f"    [CREATED]")

    except Exception as e:
        print(f"    [FAIL] {e}")
        print(f"\n  Common fixes:")
        print(f"  - Verify role exists: AmerispriseBankStepFunctionsRole")
        print(f"  - Verify role has trust for states.amazonaws.com")
        print(f"  - Verify role has inline policy from stepfunctions_role_policy.json")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  STATE MACHINE CREATED SUCCESSFULLY")
    print(f"  Name: {STATE_MACHINE_NAME}")
    print(f"  ARN:  {sm_arn}")
    print(f"  Log group: {LOG_GROUP_NAME}")
    print(f"  X-Ray tracing: ENABLED")
    print(f"\n  VIEW IN CONSOLE:")
    print(f"  https://console.aws.amazon.com/states/home?region={AWS_REGION}")
    print(f"\n  NEXT: python step2_test_manual_run.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
