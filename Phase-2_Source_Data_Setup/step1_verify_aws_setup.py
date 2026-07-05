"""
=============================================================================
PHASE 2 — STEP 1: VERIFY AWS SETUP
=============================================================================
Purpose  : Check that AWS CLI is configured correctly and boto3 can connect
Run      : python step1_verify_aws_setup.py
Expected : All checks show [PASS]
=============================================================================
"""

import boto3
import json
import sys
from botocore.exceptions import NoCredentialsError, ClientError


# ─────────────────────────────────────────────────────────────
# CONFIGURATION — Edit these values to match your setup
# ─────────────────────────────────────────────────────────────
AWS_REGION   = "ap-south-1"          # Mumbai region (closest to India)
BUCKET_NAME  = "Neo-bank-datalake"   # Your chosen bucket name


def check(label, passed, detail=""):
    status = "[PASS]" if passed else "[FAIL]"
    print(f"  {status}  {label}")
    if detail:
        print(f"          {detail}")
    return passed


def main():
    print("=" * 60)
    print("  AMERIPRISE BANK — Phase 2 AWS Setup Verification")
    print("=" * 60)
    all_ok = True

    # ── Check 1: Can boto3 load credentials? ──────────────────
    print("\n[1] Checking AWS credentials...")
    try:
        session = boto3.session.Session()
        creds   = session.get_credentials()

        if creds is None:
            ok = check("AWS credentials found", False, "No credentials found")
        else:
            frozen = creds.get_frozen_credentials()
            ok = check(
                "AWS credentials found",
                True,
                f"Access Key prefix: {frozen.access_key[:8]}..."
            )

        all_ok = all_ok and ok



    except Exception as e:
        check("AWS credentials found", False, str(e))
        all_ok = False

    # ── Check 2: Can we call STS (proves auth works)? ─────────
    print("\n[2] Checking AWS authentication...")
    try:
        sts    = boto3.client("sts", region_name=AWS_REGION)
        caller = sts.get_caller_identity()
        ok = check(
            "AWS authentication valid",
            True,
            f"Account ID : {caller['Account']}\n"
            f"          User ARN   : {caller['Arn']}"
        )
        all_ok = all_ok and ok
    except NoCredentialsError:
        check(
            "AWS authentication valid", False,
            "Run: aws configure   (enter your Access Key + Secret Key)"
        )
        all_ok = False
    except ClientError as e:
        check("AWS authentication valid", False, str(e))
        all_ok = False

    # ── Check 3: Is the region correct? ───────────────────────
    print("\n[3] Checking AWS region...")
    try:
        s3     = boto3.client("s3", region_name=AWS_REGION)
        region = boto3.session.Session().region_name
        ok = check(
            f"Region set to {AWS_REGION}",
            region is not None,
            f"Configured region: {region or 'NOT SET — run aws configure again'}"
        )
        all_ok = all_ok and ok
    except Exception as e:
        check("Region configured", False, str(e))
        all_ok = False

    # ── Check 4: Can we list S3? (proves S3 permissions) ──────
    print("\n[4] Checking S3 permissions...")
    try:
        s3      = boto3.client("s3", region_name=AWS_REGION)
        buckets = s3.list_buckets()
        names   = [b["Name"] for b in buckets["Buckets"]]
        ok = check(
            "S3 list permission working",
            True,
            f"Existing buckets found: {len(names)}  →  {names if names else '(none yet — that is fine)'}"
        )
        all_ok = all_ok and ok

        if BUCKET_NAME in names:
            check(
                f"Bucket '{BUCKET_NAME}' already exists",
                True,
                "Skip step2 bucket creation or it will say BucketAlreadyExists (that is fine)"
            )
        else:
            check(
                f"Bucket '{BUCKET_NAME}' does not exist yet",
                True,
                "Good — step2 will create it fresh"
            )

    except ClientError as e:
        check("S3 list permission working", False, str(e))
        all_ok = False

    # ── Check 5: Python packages installed? ───────────────────
    print("\n[5] Checking Python packages...")
    packages = ["boto3", "pandas", "pyarrow", "fastparquet", "tabulate"]
    for pkg in packages:
        try:
            mod = __import__(pkg)
            ver = getattr(mod, "__version__", "installed")
            check(f"{pkg}", True, f"version: {ver}")
        except ImportError:
            check(f"{pkg}", False, f"Run: pip install {pkg}")
            all_ok = False

    # ── Final summary ──────────────────────────────────────────
    print("\n" + "=" * 60)
    if all_ok:
        print("  ALL CHECKS PASSED — Ready to run Step 2!")
    else:
        print("  SOME CHECKS FAILED — Fix the [FAIL] items above")
        print("  Most common fix:  aws configure")
        print("  Then re-run this script")
    print("=" * 60)


if __name__ == "__main__":
    main()
