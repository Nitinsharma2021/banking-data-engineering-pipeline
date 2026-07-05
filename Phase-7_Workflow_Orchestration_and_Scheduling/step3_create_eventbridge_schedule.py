"""
=============================================================================
PHASE 7 — STEP 3: CREATE EVENTBRIDGE SCHEDULE
=============================================================================
Purpose : Create daily 02:00 IST schedule that triggers the state machine.
Run     : python step3_create_eventbridge_schedule.py
=============================================================================
"""

import json
import sys
import boto3
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# UPDATE THESE VALUES
# ─────────────────────────────────────────────────────────────
ACCOUNT_ID         = "843302972838"
SCHEDULER_ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/AmerispriseBankSchedulerRole"
# ─────────────────────────────────────────────────────────────

AWS_REGION         = "ap-south-1"
SCHEDULE_NAME      = "neo-bank-daily-etl"
STATE_MACHINE_NAME = "neo-bank-etl-orchestrator"
STATE_MACHINE_ARN  = f"arn:aws:states:{AWS_REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"

# Daily 02:00 IST = 20:30 UTC previous day
# Using Asia/Kolkata timezone so cron is in local time
SCHEDULE_EXPRESSION = "cron(0 2 * * ? *)"
TIMEZONE            = "Asia/Kolkata"


def main():
    print("=" * 60)
    print("  PHASE 7 STEP 3: Create EventBridge Schedule")
    print("=" * 60)

    print(f"\n  Configuration:")
    print(f"  Schedule name:    {SCHEDULE_NAME}")
    print(f"  Cron expression:  {SCHEDULE_EXPRESSION}")
    print(f"  Timezone:         {TIMEZONE}")
    print(f"  Target:           {STATE_MACHINE_NAME}")
    print(f"  Role:             AmerispriseBankSchedulerRole")

    sched = boto3.client("scheduler", region_name=AWS_REGION)

    target_input = json.dumps({
        "run_date":    "<aws.scheduler.scheduled-time>",
        "trigger":     "scheduled",
        "triggered_by": "eventbridge-scheduler"
    })

    target_config = {
        "Arn":     STATE_MACHINE_ARN,
        "RoleArn": SCHEDULER_ROLE_ARN,
        "Input":   target_input,
        "RetryPolicy": {
            "MaximumEventAgeInSeconds": 3600,
            "MaximumRetryAttempts":     2
        }
    }

    try:
        existing = sched.get_schedule(Name=SCHEDULE_NAME, GroupName="default")
        print(f"\n[!] Schedule already exists. Updating...")

        sched.update_schedule(
            Name=SCHEDULE_NAME,
            GroupName="default",
            ScheduleExpression=SCHEDULE_EXPRESSION,
            ScheduleExpressionTimezone=TIMEZONE,
            FlexibleTimeWindow={"Mode": "OFF"},
            State="ENABLED",
            Target=target_config,
            Description="Ameriprise Bank daily ETL pipeline trigger"
        )
        action = "UPDATED"

    except sched.exceptions.ResourceNotFoundException:
        print(f"\n[1] Creating schedule...")

        try:
            sched.create_schedule(
                Name=SCHEDULE_NAME,
                GroupName="default",
                ScheduleExpression=SCHEDULE_EXPRESSION,
                ScheduleExpressionTimezone=TIMEZONE,
                FlexibleTimeWindow={"Mode": "OFF"},
                State="ENABLED",
                Target=target_config,
                Description="Ameriprise Bank daily ETL pipeline trigger"
            )
            action = "CREATED"
        except Exception as e:
            print(f"    [FAIL] {e}")
            print(f"\n  Common fixes:")
            print(f"  - Verify AmerispriseBankSchedulerRole exists")
            print(f"  - Verify trust policy uses scheduler.amazonaws.com")
            print(f"    NOT events.amazonaws.com (that is for legacy rules)")
            print(f"  - Verify the role has states:StartExecution permission")
            sys.exit(1)

    print(f"    [{action}]")

    print(f"\n[2] Verifying...")
    resp = sched.get_schedule(Name=SCHEDULE_NAME, GroupName="default")
    print(f"    Status:           {resp['State']}")
    print(f"    Schedule:         {resp['ScheduleExpression']}")
    print(f"    Timezone:         {resp.get('ScheduleExpressionTimezone', 'UTC')}")
    print(f"    Created at:       {resp['CreationDate'].isoformat()}")

    print(f"\n{'='*60}")
    print(f"  SCHEDULE CREATED")
    print(f"  Pipeline runs daily at 02:00 IST")
    print(f"\n  WHEN IT WILL RUN:")
    today = datetime.now()
    if today.hour < 2:
        next_run_today = today.replace(hour=2, minute=0, second=0, microsecond=0)
        print(f"    Next run: today {next_run_today.strftime('%Y-%m-%d 02:00 IST')}")
    else:
        from datetime import timedelta
        next_run = (today + timedelta(days=1)).replace(hour=2, minute=0, second=0, microsecond=0)
        print(f"    Next run: tomorrow {next_run.strftime('%Y-%m-%d 02:00 IST')}")

    print(f"\n  VIEW IN CONSOLE:")
    print(f"  https://console.aws.amazon.com/scheduler/home?region={AWS_REGION}#schedules")

    print(f"\n  TO DISABLE THE SCHEDULE TEMPORARILY:")
    print(f"  aws scheduler update-schedule \\")
    print(f"    --name {SCHEDULE_NAME} \\")
    print(f"    --state DISABLED ...")

    print(f"\n  NEXT: python step4_verify_orchestration.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
