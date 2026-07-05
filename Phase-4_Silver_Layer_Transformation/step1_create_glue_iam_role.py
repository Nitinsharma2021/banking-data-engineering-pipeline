"""
=============================================================================
PHASE 4 — STEP 1: CREATE IAM ROLE FOR GLUE JOBS
=============================================================================
Purpose  : Create the IAM Role that all 6 Glue Visual ETL jobs will use
           to access S3, write CloudWatch logs, and run Glue operations
Run      : python step1_create_glue_iam_role.py
Expected : Role "ApBankGlueRole" created — copy the ARN printed

WHY IAM ROLE:
  Glue jobs need permission to:
  1. Read from S3 bronze/ zone
  2. Write to S3 silver/ and quarantine/ zones
  3. Write logs to CloudWatch (for debugging)
  4. Access Glue Data Catalog (to register silver tables)
=============================================================================
"""

import boto3
import json
from botocore.exceptions import ClientError


AWS_REGION = "ap-south-1"
ROLE_NAME  = "AmeripriseBankGlueRole"

# Trust policy: allows Glue service to assume this role
TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }
    ]
}

# Inline policy: specific S3 permissions for our bucket
INLINE_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3BucketAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                "s3:ListBucket", "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads", "s3:AbortMultipartUpload"
            ],
            "Resource": [
                "arn:aws:s3:::neo-bank-datalake",
                "arn:aws:s3:::neo-bank-datalake/*"
            ]
        },
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "logs:*",
                "cloudwatch:*"
            ],
            "Resource": "*"
        }
    ]
}

MANAGED_POLICIES = [
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
    "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    "arn:aws:iam::aws:policy/CloudWatchFullAccess",
]


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 4 Step 1: Create Glue IAM Role")
    print("=" * 65)

    iam = boto3.client("iam", region_name=AWS_REGION)

    # ── Create the role ─────────────────────────────────────
    print(f"\n[1] Creating IAM Role: {ROLE_NAME}...")
    try:
        resp = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description="IAM role for Ameriprise Bank Glue ETL jobs - Phase 4",
            Tags=[
                {"Key": "Project",     "Value": "AmeripriseBankDEPipeline"},
                {"Key": "Phase",       "Value": "Phase4-GlueETL"},
                {"Key": "ManagedBy",   "Value": "DataEngineering"},
            ]
        )
        role_arn = resp["Role"]["Arn"]
        print(f"  [CREATED] Role ARN: {role_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            resp     = iam.get_role(RoleName=ROLE_NAME)
            role_arn = resp["Role"]["Arn"]
            print(f"  [EXISTS]  Role already exists")
            print(f"            Role ARN: {role_arn}")
        else:
            raise e

    # ── Attach managed policies ──────────────────────────────
    print(f"\n[2] Attaching managed policies...")
    for policy_arn in MANAGED_POLICIES:
        try:
            iam.attach_role_policy(RoleName=ROLE_NAME, PolicyArn=policy_arn)
            print(f"  [OK]  {policy_arn.split('/')[-1]}")
        except ClientError as e:
            if "already attached" in str(e).lower():
                print(f"  [OK]  {policy_arn.split('/')[-1]} (already attached)")
            else:
                print(f"  [WARN] {e}")

    # ── Add inline S3 bucket policy ──────────────────────────
    print(f"\n[3] Adding inline S3 bucket policy...")
    try:
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName="AmerispriseBankS3BucketPolicy",
            PolicyDocument=json.dumps(INLINE_POLICY)
        )
        print(f"  [OK]  Inline policy attached")
    except ClientError as e:
        print(f"  [WARN] {e}")

    # ── Verify ───────────────────────────────────────────────
    print(f"\n[4] Verifying role...")
    resp     = iam.get_role(RoleName=ROLE_NAME)
    policies = iam.list_attached_role_policies(RoleName=ROLE_NAME)
    print(f"  Role Name : {resp['Role']['RoleName']}")
    print(f"  Role ARN  : {resp['Role']['Arn']}")
    print(f"  Policies  :")
    for p in policies["AttachedPolicies"]:
        print(f"    - {p['PolicyName']}")

    print(f"\n{'='*65}")
    print(f"  ROLE CREATED SUCCESSFULLY")
    print(f"\n  COPY THIS ROLE ARN — needed when creating Glue jobs:")
    print(f"\n  {resp['Role']['Arn']}")
    print(f"\n  In Glue Visual ETL → Job Details → IAM Role:")
    print(f"  Select: AmerispriseBankGlueRole")
    print(f"\n  NEXT: Create Glue database in console")
    print(f"        Then create all 6 Visual ETL jobs")
    print("=" * 65)


if __name__ == "__main__":
    main()
