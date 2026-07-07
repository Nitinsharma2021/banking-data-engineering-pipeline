# 🏦 NeoBank — End-to-End Banking Data Engineering Pipeline

[![AWS](https://img.shields.io/badge/AWS-Glue%20%7C%20S3%20%7C%20Redshift-orange?logo=amazonaws)](https://aws.amazon.com/)
[![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-ETL-red?logo=apachespark)](https://spark.apache.org/)
[![Status](https://img.shields.io/badge/status-active-brightgreen)]()
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

A production-style, end-to-end data engineering pipeline for a fictional digital bank ("NeoBank"), built entirely on **AWS** using a **Medallion (Bronze → Silver → Gold) Architecture**. The pipeline ingests raw banking data (customers, accounts, transactions, payments), applies data quality checks and PII masking, builds a star-schema data warehouse in **Redshift Serverless**, and exposes it for ad-hoc analytics via **Athena** and **Apache Superset**.

---

## 📌 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Data Model](#-data-model)
- [Project Structure](#-project-structure)
- [Pipeline Walkthrough](#-pipeline-walkthrough)
- [Data Quality & PII Handling](#-data-quality--pii-handling)
- [Orchestration & Scheduling](#-orchestration--scheduling)
- [Monitoring & Alerting](#-monitoring--alerting)
- [Screenshots](#-screenshots)
- [Setup & Deployment](#-setup--deployment)
- [Sample Queries](#-sample-queries)
- [Future Improvements](#-future-improvements)
- [Author](#-author)

---

## 📖 Overview

NeoBank simulates a **real banking data platform** that a Data Engineering team would build and operate in production. This project demonstrates end-to-end data engineering best practices in the BFSI (Banking, Financial Services & Insurance) domain.

### Project Goals

✅ Design and implement a **Medallion Architecture** (Bronze/Silver/Gold) on **Amazon S3**  
✅ Build **AWS Glue Visual ETL** jobs for ingestion, cleansing, PII masking, and aggregation  
✅ Enforce **data quality rules** with failed record quarantine for audit trails  
✅ Model a **star schema** (fact + dimension tables, SCD Type 2) for analytics  
✅ Load curated data into **Redshift Serverless** for BI and ad-hoc querying  
✅ Automate the entire pipeline with **EventBridge + Step Functions**  
✅ Add **CloudWatch monitoring**, anomaly detection, and **SNS alerting**  

> This reflects real BFSI practices: incremental extraction with watermarking, PII masking at transformation layer, immutable audit trails, and regulatory-style reporting tables.

---

## 🏗 Architecture

### Data Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                       NEOBANK DATA PIPELINE                             │
└─────────────────────────────────────────────────────────────────────────┘

INGESTION LAYER
    ↓
[RDS SQL Server]     [S3 CSV Landing]
  • branches           • payment_gateway
  • customers          • credit_bureau
  • accounts           
  • transactions       
    │                  │
    └──────┬───────────┘
           ↓
    BRONZE ZONE (S3)
    • Raw Parquet
    • Immutable & versioned
    • Append-only writes
    • Watermark tracking
           ↓
    AWS GLUE VISUAL ETL
    • 7 DQ rules per table
    • PII masking (PAN, Email, Phone)
    • Enrichment (risk_band)
    • Partitioned by load_date
           ↓
    ┌──────┴──────┐
    ↓             ↓
SILVER ZONE    QUARANTINE ZONE
Clean Data     Failed Records
              (For audit & reprocessing)
    │
    ↓
AWS GLUE ETL (FACT/DIMENSION JOINS)
    • Star schema modeling
    • SCD Type 2 (dim_customer)
    • Aggregations (360 views, daily balances)
    │
    ↓
    GOLD ZONE (S3)
    • Star Schema Parquet
    • Fact tables (transactions, payments)
    • Dimension tables (customer, account, branch)
    • Aggregate tables (daily, monthly, customer 360)
    │
    ↓
REDSHIFT SERVERLESS
    • COPY commands load Gold Zone Parquet
    • banking schema + analytical views
    • SCD Type 2 dimensions tracked
    │
    ↓
    ┌──────────────────────┬──────────────┐
    ↓                      ↓              ↓
APACHE SUPERSET      AWS ATHENA    CUSTOM DASHBOARDS
(EC2-hosted)         (Direct S3)    (Analytics Layer)
```

### Architecture Diagram

![NeoBank Architecture](Assest/WhatsApp%20Image%202026-07-07%20at%207.18.54%20PM.jpeg)

---

## 🧰 Tech Stack

| Layer | Tools / Services |
|---|---|
| **Ingestion** | AWS RDS (SQL Server), S3 (CSV landing) |
| **Storage / Lake** | Amazon S3 (Bronze / Silver / Gold / Quarantine zones), Parquet |
| **ETL / Transformation** | AWS Glue Visual ETL, PySpark, AWS Glue Data Catalog |
| **Data Warehouse** | Amazon Redshift Serverless |
| **Orchestration** | Amazon EventBridge Scheduler, AWS Step Functions |
| **Ad-hoc Analytics** | Apache Superset (EC2-hosted), AWS Athena |
| **Monitoring & Alerting** | Amazon CloudWatch (Dashboards + Alarms), AWS Lambda, Amazon SNS |
| **Languages** | Python, PySpark, SQL |

---

## 🗂 Data Model

The Gold Zone follows a **star schema** design optimized for analytics queries on Redshift.

### Dimension Tables (SCD Type 2 for Customer)

| Table | Purpose | Key Columns |
|-------|---------|------------|
| `dim_customer` | Customer master with historical tracking | customer_id, name, risk_band, is_current, effective_date, end_date |
| `dim_account` | Account types and status | account_id, account_type, status, open_date |
| `dim_branch` | Branch locations and details | branch_id, branch_name, city, region |
| `dim_date` | Calendar dimension | date_id, date, month, quarter, year, day_of_week |

### Fact Tables (Transactional & Risk)

| Table | Purpose | Grain | Key Metrics |
|-------|---------|-------|------------|
| `fact_transactions` | All customer transactions | Transaction level | amount, status, timestamp |
| `fact_payments` | Payment records and status | Payment level | payment_amount, fee, status |
| `fact_credit_risk` | Credit risk scoring | Customer-monthly | risk_score, dpd, npa_flag |

### Aggregate / Reporting Tables (Pre-computed)

| Table | Refresh Frequency | Use Case |
|-------|------------------|----------|
| `agg_customer_360` | Daily | Comprehensive customer view (balance, transactions, risk) |
| `agg_daily_balances` | Daily | Account balance history for trend analysis |
| `agg_branch_performance` | Daily | Branch KPIs (transaction count, volume, fees) |
| `agg_monthly_summary` | Monthly | Executive reporting and compliance views |

---

## 📁 Project Structure

```
Financial_Banking_Data_engineering_project/
│
├── glue_jobs/                      # AWS Glue Visual ETL Job Scripts
│   ├── bronze_ingestion.py         # Incremental RDS → Bronze, CSV → Bronze
│   ├── silver_dq_pii_masking.py    # DQ enforcement, PII masking, enrichment
│   └── gold_fact_dim_build.py      # Star schema joins, aggregations
│
├── data_quality/                   # Data Quality Frameworks
│   ├── dq_rules.json               # 7 DQ rules per table (nulls, duplicates, format checks)
│   └── quarantine_handler.py       # Routes failed records to S3 quarantine zone
│
├── redshift/                       # Redshift Serverless DDL & COPY
│   ├── create_schema.sql           # Creates banking schema + tables
│   ├── copy_commands.sql           # COPY commands for Gold → Redshift load
│   └── analytical_views.sql        # Pre-aggregated views for BI layer
│
├── orchestration/                  # Workflow Orchestration
│   ├── state_machine.json          # AWS Step Functions state machine (DAG)
│   └── eventbridge_scheduler.json  # EventBridge Scheduler (triggers daily at 02:00 IST)
│
├── monitoring/                     # Observability & Alerting
│   ├── cloudwatch_dashboards.json  # 3 CloudWatch dashboards (pipeline, job-level, data quality)
│   ├── cloudwatch_alarms.json      # 12 alarms (failures, latency, anomalies)
│   └── custom_metrics_lambda.py    # Custom metrics for DQ pass/fail rates
│
├── assets/                         # Screenshots & Diagrams
│   └── *.jpeg                      # Architecture & pipeline screenshots
│
└── README.md                       # This file
```

### Key Files Explained

- **bronze_ingestion.py** — Watches RDS and S3 for new data, writes immutable versioned Parquet
- **silver_dq_pii_masking.py** — Validates 7 DQ rules, masks PII, enriches data, routes failures to quarantine
- **gold_fact_dim_build.py** — Joins facts/dimensions, builds star schema, pre-aggregates tables
- **state_machine.json** — Orchestrates: Bronze → Silver → Gold → Redshift in sequence
- **custom_metrics_lambda.py** — Publishes DQ metrics to CloudWatch for alerting

---

## 🔄 Pipeline Walkthrough

### Step 1: Ingestion
**Source:** RDS SQL Server + S3 CSV Landing  
**Process:** Incremental extraction with watermark tracking
```
RDS branches, customers, accounts, transactions  →  Watermark tracked by load_date
S3 CSV payment_gateway, credit_bureau           →  File ingestion pattern
```

### Step 2: Bronze Zone (S3 Raw Layer)
**Characteristics:**
- Raw, immutable, versioned Parquet format
- Append-only writes (no updates)
- Watermark tracking for incremental loads
- Complete audit trail with `_loaded_date` and `_version` columns

### Step 3: AWS Glue ETL — Silver Zone
**Data Transformations:**
- ✅ **Data Quality Enforcement** — 7 rules per table (nulls, referential integrity, format checks, duplicates)
- ✅ **PII Masking** — Masks PAN numbers, email addresses, phone numbers
- ✅ **Data Enrichment** — Adds computed columns (e.g., `risk_band` from credit score)
- ✅ **Partitioning** — Organized by `load_date` for efficient querying
- ❌ **Failed Records** → Routed to dedicated **Quarantine S3 Zone** (never dropped, preserved for audit)

### Step 4: AWS Glue ETL — Gold Zone
**Dimensional Modeling:**
- Builds **Fact tables** from cleaned Silver data
- Joins with **Dimension tables** (customer, account, branch)
- Implements **SCD Type 2** for `dim_customer` (tracks historical changes)
- Pre-aggregates into **reporting tables** (customer_360, daily_balances, branch_performance)
- Output: Star-schema Parquet files in Gold Zone S3

### Step 5: Redshift Serverless Load
**SQL COPY Commands:**
```sql
COPY banking.fact_transactions FROM 's3://bucket/gold/fact_transactions.parquet';
COPY banking.dim_customer FROM 's3://bucket/gold/dim_customer.parquet';
-- ... similar for all dimension & fact tables
```

### Step 6: Analytics & Reporting
**Query Layer Options:**
- **Apache Superset** (EC2-hosted) — Interactive dashboards, drill-downs
- **AWS Athena** — Direct SQL queries on Gold Zone Parquet (no load needed)
- **Redshift Query Editor** — Complex joins and aggregations on loaded data

---

## ✅ Data Quality & PII Handling

### Data Quality Framework

| Rule Type | Per-Table Count | Implementation | Failed Records |
|-----------|-----------------|-----------------|---|
| Null checks | 2–3 per table | WHERE col IS NULL | → Quarantine Zone |
| Referential integrity | 1–2 per table | Foreign key validation | → Quarantine Zone |
| Format validation | 1–2 per table | Regex / data type checks | → Quarantine Zone |
| Duplicate detection | 1 per table | GROUP BY business keys | → Quarantine Zone |
| **Total per table** | **7 rules** | AWS Glue Visual ETL | **Never dropped** |

### PII Masking Strategy

Implemented at the **Silver layer** (before any downstream consumption):

```python
# Example: PII Masking in Glue Job
customer.pan_number        → MASKED_PAN_****1234      # Keep last 4 for verification
customer.email_address     → cust_***@example.com     # Hash + show domain
customer.phone_number      → +91-XXXX-****89         # Keep country code + last 2 digits
customer.street_address    → [REDACTED]               # Full removal if not needed
```

### Audit & Compliance

- **Immutable Bronze Zone** — All raw data retained forever (audit trail)
- **Versioning ON** — Track data changes over time with `_version` column
- **Quarantine Zone** — Failed records preserved with metadata (rule failed, timestamp, reason)
- **Data Lineage** — AWS Glue Data Catalog tracks table relationships and transformations

---

## ⏱ Orchestration & Scheduling

### EventBridge Scheduler
- **Trigger:** Daily at **02:00 IST** (off-peak hours)
- **Target:** AWS Step Functions state machine

### AWS Step Functions (State Machine)
```
START
  │
  ├─→ [Glue Job] bronze_ingestion.py ──→ RDS → S3 Bronze
  │
  ├─→ [Glue Job] silver_dq_pii_masking.py ──→ Bronze → Silver (+ Quarantine)
  │
  ├─→ [Glue Job] gold_fact_dim_build.py ──→ Silver → Gold (Star Schema)
  │
  ├─→ [Redshift] COPY commands ──→ Gold Parquet → Redshift banking schema
  │
  └─→ [SNS Notification] ──→ SUCCESS or FAILURE alert to data team
```

### Notification Pattern

| Event | Channel | Recipients |
|-------|---------|---|
| ✅ Pipeline Success | SNS Email | data-eng@company.com |
| ❌ Job Failure | SNS Email + SMS | on-call-de@company.com |
| ⚠️ DQ Anomaly | CloudWatch Alarm | data-quality-team@company.com |

---

## 📊 Monitoring & Alerting

### CloudWatch Dashboards (3 Total)

| Dashboard | Metrics Tracked | Refresh |
|-----------|-----------------|---------|
| **Pipeline Health** | Job status, run duration, record counts (Bronze → Gold) | Real-time |
| **Job Performance** | Glue job execution time, DPU usage, shuffle stats | Real-time |
| **Data Quality** | DQ pass/fail rates per table, quarantine record count | Daily |

### CloudWatch Alarms (12 Total)

| Alarm Name | Threshold | Alert Severity |
|-----------|-----------|---|
| Glue Job Failure | Any job fails | **CRITICAL** |
| Long Job Duration | > 45 min | WARNING |
| High Quarantine Count | > 1000 records | WARNING |
| Redshift Copy Failure | Any COPY fails | **CRITICAL** |
| DQ Pass Rate Drop | < 95% on table | WARNING |
| Missing Records | Table count ↓ 20% | WARNING |
| Anomaly Detected | ML-based threshold | INFO |
| SNS Publish Failure | Notification fails | **CRITICAL** |

### Custom Metrics & Lambda

**Custom Metrics Lambda** (`custom_metrics_lambda.py`):
- Reads quarantine zone S3
- Computes: `DQ_PassRate = (records_processed - quarantined) / records_processed`
- Publishes to CloudWatch under `NeoBank/DataQuality` namespace
- Triggered after each Glue job completes

### Alerts Flow

```
CloudWatch Alarm triggered
           ↓
   SNS Topic activated
           ↓
    ┌──────┴──────┐
    ↓             ↓
Email Alert    SMS Alert
(team inbox)  (on-call phone)
```

---

## 🖼 Screenshots

### Pipeline Execution & Query Layer

| Redshift Query Editor v2 | SageMaker Unified Studio |
|---|---|
| ![Redshift Schema](Assest/WhatsApp%20Image%202026-07-07%20at%207.18.54%20PM.jpeg) | ![SageMaker Studio](Assest/WhatsApp%20Image%202026-07-07%20at%207.19.15%20PM.jpeg) |

### Infrastructure & Monitoring

| Step Functions Execution | CloudWatch Dashboard |
|---|---|
| ![Pipeline Flow](Assest/WhatsApp%20Image%202026-07-07%20at%207.19.36%20PM.jpeg) | ![Monitoring](Assest/WhatsApp%20Image%202026-07-07%20at%207.21.10%20PM.jpeg) |

### Additional Dashboards

| Superset Analytics | AWS Glue Jobs |
|---|---|
| ![Superset Dashboard](Assest/WhatsApp%20Image%202026-07-07%20at%207.34.47%20PM.jpeg) | ![Glue Console](Assest/WhatsApp%20Image%202026-07-07%20at%207.35.32%20PM.jpeg) |

### Architecture Diagram

![Full Architecture](Assest/WhatsApp%20Image%202026-07-07%20at%207.36.17%20PM.jpeg)

---

## ⚙️ Setup & Deployment

> Update this section with your actual deployment steps.




---

## 🔎 Sample Queries

```sql
-- Customer 360 view: total balance, transaction count, risk band
SELECT
    c.customer_id,
    c.risk_band,
    SUM(t.amount) AS total_transaction_value,
    COUNT(t.transaction_id) AS transaction_count
FROM banking.fact_transactions t
JOIN banking.dim_customer c
    ON t.customer_id = c.customer_id
    AND c.is_current = TRUE
GROUP BY c.customer_id, c.risk_band
ORDER BY total_transaction_value DESC
LIMIT 10;
```

---

## 🚀 Future Improvements

- [ ] **Add dbt** for transformation testing, documentation, and lineage
- [ ] **Real-time Streaming** — Kinesis → Glue Streaming for fraud detection low-latency path
- [ ] **CI/CD Pipeline** — GitHub Actions to auto-deploy Glue jobs on PR merge
- [ ] **Data Lineage** — Integrate OpenLineage or Collibra for end-to-end tracking
- [ ] **Machine Learning** — SageMaker integration for customer churn / credit risk models
- [ ] **Incremental Redshift Loads** — Implement merge logic instead of full COPY
- [ ] **Apache Iceberg** — Replace Parquet with Iceberg for ACID compliance
- [ ] **Cost Optimization** — S3 Intelligent-Tiering, Glue job auto-scaling tuning

---


⭐️ If you found this project useful or interesting, consider giving it a star!
