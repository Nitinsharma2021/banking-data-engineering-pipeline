========================================
PHASE 1: AWS INFRASTRUCTURE & SOURCE SETUP
========================================

1. AWS CLI & IAM SETUP
   - IAM user with programmatic access (Access Key + Secret Key)
   - AWS CLI configured: aws configure
   - Default region: ap-south-1 (Mumbai)

2. S3 DATA LAKE BUCKETS
   - s3://banking-data-bronze/      (raw landing zone)
   - s3://banking-data-silver/      (cleansed zone)
   - s3://banking-data-gold/        (analytics-ready)
   - s3://banking-pipeline-scripts/ (Python/Spark scripts)
   - s3://banking-pipeline-logs/    (execution logs)
   - Folder structure: zone/entity/year/month/day/

3. VPC & NETWORKING
   - VPC with 2 public subnets + 2 private subnets
   - Internet Gateway attached
   - NAT Gateway for private subnet outbound access
   - Security Groups:
     * RDS-SG: Port 5432, inbound from pipeline only
     * Redshift-SG: Port 5439, restricted access

4. RDS POSTGRESQL — SOURCE DATABASE
   - Engine: PostgreSQL 14
   - Instance: db.t3.micro / db.t3.small
   - Database name: banking
   - Schema: banking
   - Deployment: Private subnet (no public access)

5. SOURCE TABLES CREATED
   - banking.branches      (branch reference data)
   - banking.customers     (KYC & PII data)
   - banking.accounts      (account balances & types)
   - banking.transactions  (transaction ledger)

6. IAM ROLES & POLICIES
   - GlueServiceRole: S3 read/write, RDS read, CloudWatch logs
   - StepFunctionsRole: Invoke Lambda, Glue jobs, pass roles
   - RedshiftRole: S3 read access for COPY commands
   - EventBridgeRole: Trigger Step Functions on schedule

7. HISTORICAL DATA SEEDING
   - 4 branches: Mumbai, Delhi, Bangalore, Ahmedabad
   - ~500 customers
   - ~1,000 accounts
   - ~30,000 historical transactions
   - KYC statuses: VERIFIED, PENDING, REJECTED
   - Account types: Savings, Current

8. EXTERNAL DATA SOURCE SETUP
   - CSV templates for payment_gateway_logs
   - CSV templates for credit_bureau_reports
   - Simulated landing folder in S3 for file-based ingestion

9. SECURITY & GOVERNANCE BASELINE
   - S3 bucket versioning enabled
   - S3 bucket encryption: SSE-S3
   - S3 block public access: All buckets
   - RDS publicly accessible: No
   - CloudWatch Log Groups created for pipeline monitoring

========================================