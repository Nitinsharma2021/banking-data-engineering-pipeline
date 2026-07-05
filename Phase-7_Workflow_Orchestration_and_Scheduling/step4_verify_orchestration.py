"""
=============================================================================
PHASE 7 — STEP 4: VERIFY COMPLETE ORCHESTRATION
=============================================================================
Purpose : Verify all components of Phase 7 are correctly configured.
Run     : python step4_verify_orchestration.py
=============================================================================
"""

import boto3
from datetime import datetime, timezone


ACCOUNT_ID         = "843302972838"
AWS_REGION         = "ap-south-1"

STATE_MACHINE_NAME = "neo-bank-etl-orchestrator"
STATE_MACHINE_ARN  = f"arn:aws:states:{AWS_REGION}:{ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"

SCHEDULE_NAME      = "neo-bank-daily-etl"
SNS_TOPIC_NAME     = "neo-bank-etl-alerts"
SNS_TOPIC_ARN      = f"arn:aws:sns:{AWS_REGION}:{ACCOUNT_ID}:{SNS_TOPIC_NAME}"
LOG_GROUP_NAME     = "/aws/vendedlogs/states/neo-bank-etl"


def format_duration(seconds):
    minutes = int(seconds // 60)
    secs    = int(seconds % 60)
    return f"{minutes}m {secs}s"


def check_state_machine(sfn):
    print(f"\n[1] State Machine")
    try:
        resp = sfn.describe_state_machine(stateMachineArn=STATE_MACHINE_ARN)
        print(f"  ✓ Name:    {resp['name']}")
        print(f"  ✓ Status:  {resp['status']}")
        print(f"  ✓ Type:    {resp['type']}")
        print(f"  ✓ Tracing: {resp['tracingConfiguration']['enabled']}")
        print(f"  ✓ Logging: {resp['loggingConfiguration']['level']}")
        return resp["status"] == "ACTIVE"
    except Exception as e:
        print(f"  ✗ State machine not found: {e}")
        return False


def check_recent_executions(sfn):
    print(f"\n[2] Recent Executions (last 5)")
    try:
        resp = sfn.list_executions(stateMachineArn=STATE_MACHINE_ARN, maxResults=5)
        executions = resp.get("executions", [])

        if not executions:
            print(f"  ! No executions yet — run step2_test_manual_run.py first")
            return False

        for ex in executions:
            status = ex["status"]
            sym    = "✓" if status == "SUCCEEDED" else ("…" if status == "RUNNING" else "✗")
            name   = ex["name"][:40]

            if "stopDate" in ex:
                duration = (ex["stopDate"] - ex["startDate"]).total_seconds()
                dur_str  = format_duration(duration)
            else:
                dur_str = "running"

            print(f"  {sym} {name:<42} {status:<10} {dur_str}")
        return True
    except Exception as e:
        print(f"  ✗ Cannot list executions: {e}")
        return False


def check_schedule():
    print(f"\n[3] EventBridge Schedule")
    sched = boto3.client("scheduler", region_name=AWS_REGION)
    try:
        resp = sched.get_schedule(Name=SCHEDULE_NAME, GroupName="default")
        print(f"  ✓ Name:       {resp['Name']}")
        print(f"  ✓ State:      {resp['State']}")
        print(f"  ✓ Cron:       {resp['ScheduleExpression']}")
        print(f"  ✓ Timezone:   {resp.get('ScheduleExpressionTimezone', 'UTC')}")
        print(f"  ✓ Target:     {resp['Target']['Arn'].split(':')[-1]}")
        return resp["State"] == "ENABLED"
    except sched.exceptions.ResourceNotFoundException:
        print(f"  ✗ Schedule not found — run step3_create_eventbridge_schedule.py")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def check_sns():
    print(f"\n[4] SNS Topic + Subscriptions")
    sns = boto3.client("sns", region_name=AWS_REGION)
    try:
        sns.get_topic_attributes(TopicArn=SNS_TOPIC_ARN)
        print(f"  ✓ Topic: {SNS_TOPIC_NAME}")

        resp  = sns.list_subscriptions_by_topic(TopicArn=SNS_TOPIC_ARN)
        subs  = resp.get("Subscriptions", [])

        if not subs:
            print(f"  ! No subscribers — add an email subscription")
            return False

        confirmed = 0
        pending   = 0
        for s in subs:
            arn = s["SubscriptionArn"]
            if arn == "PendingConfirmation":
                pending += 1
                print(f"  ! Pending: {s['Protocol']} → {s['Endpoint']}")
            else:
                confirmed += 1
                print(f"  ✓ Active:  {s['Protocol']} → {s['Endpoint']}")

        return confirmed > 0
    except Exception as e:
        print(f"  ✗ SNS topic not found: {e}")
        return False


def check_cloudwatch_logs():
    print(f"\n[5] CloudWatch Logs")
    logs = boto3.client("logs", region_name=AWS_REGION)
    try:
        resp = logs.describe_log_groups(logGroupNamePrefix=LOG_GROUP_NAME)
        groups = resp.get("logGroups", [])

        for g in groups:
            if g["logGroupName"] == LOG_GROUP_NAME:
                size_mb = g.get("storedBytes", 0) / (1024 * 1024)
                print(f"  ✓ Log group: {LOG_GROUP_NAME}")
                print(f"    Size: {size_mb:.2f} MB")
                return True

        print(f"  ! Log group not found")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def check_glue_jobs():
    print(f"\n[6] Glue Jobs Existence")
    glue = boto3.client("glue", region_name=AWS_REGION)

    expected = [
        "bronze_to_silver_customers",
        "bronze_to_silver_accounts",
        "bronze_to_silver_transactions",
        "bronze_to_silver_payment_gateway",
        "bronze_to_silver_credit_bureau",
        "bronze_to_silver_branches",
        "silver_to_gold_dim_branch",
        "silver_to_gold_dim_customer",
        "silver_to_gold_dim_account",
        "silver_to_gold_fact_transactions",
        "silver_to_gold_fact_payments",
        "silver_to_gold_fact_credit_risk",
        "silver_to_gold_aggregations",
    ]

    missing = []
    for job_name in expected:
        try:
            glue.get_job(JobName=job_name)
            print(f"  ✓ {job_name}")
        except glue.exceptions.EntityNotFoundException:
            print(f"  ✗ {job_name} NOT FOUND")
            missing.append(job_name)
        except Exception as e:
            print(f"  ? {job_name} — {e}")

    if missing:
        print(f"\n  [WARN] Missing Glue jobs will cause state machine failures")
        return False
    return True


def main():
    print("=" * 60)
    print("  PHASE 7 STEP 4: Verify Orchestration")
    print(f"  Time: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)

    sfn = boto3.client("stepfunctions", region_name=AWS_REGION)

    results = []
    results.append(("State Machine",      check_state_machine(sfn)))
    results.append(("Recent Executions",  check_recent_executions(sfn)))
    results.append(("EventBridge Schedule", check_schedule()))
    results.append(("SNS Topic",          check_sns()))
    results.append(("CloudWatch Logs",    check_cloudwatch_logs()))
    results.append(("Glue Jobs",          check_glue_jobs()))

    print(f"\n{'='*60}")
    print(f"  PHASE 7 VERIFICATION SUMMARY")
    print(f"{'='*60}")

    passed = 0
    for name, ok in results:
        sym    = "✓" if ok else "✗"
        status = "PASS" if ok else "FAIL"
        print(f"  {sym} {name:<25} {status}")
        if ok:
            passed += 1

    print(f"\n  {passed}/{len(results)} checks passed")

    if passed == len(results):
        print(f"\n  PHASE 7 COMPLETE!")
        print(f"  Daily orchestration is live.")
        print(f"  Pipeline will run automatically at 02:00 IST every day.")
        print(f"\n  READY FOR PHASE 8: CloudWatch Monitoring + Alerting")
    else:
        print(f"\n  Some checks failed. Re-check failed items.")
    print("=" * 60)


if __name__ == "__main__":
    main()
