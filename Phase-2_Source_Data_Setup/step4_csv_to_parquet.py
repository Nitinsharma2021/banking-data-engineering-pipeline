"""
=============================================================================
PHASE 2 — STEP 4: CONVERT CSV FILES → PARQUET WITH METADATA COLUMNS
=============================================================================
Purpose  : Read your 4 CSV files, add pipeline metadata columns,
           and save as Parquet format (the standard format for data lakes)
Run      : python step4_csv_to_parquet.py
Expected : 4 Parquet files created in local output/ folder

WHY PARQUET INSTEAD OF CSV?
  CSV  → plain text, no type info, 100 MB file takes 100 MB storage
  Parquet → compressed binary, keeps data types, same 100 MB file = ~15-20 MB
  Parquet is also 10x faster to query in Athena / Redshift.
  ALL data in your data lake will be Parquet — not CSV.

METADATA COLUMNS ADDED TO EVERY FILE:
  src_system     → Where the data came from (e.g. "payment_gateway_csv")
  src_file_name  → Original filename (for traceability)
  load_timestamp → When this record was loaded into the pipeline
  batch_id       → UUID for this specific pipeline run (groups all records together)
  row_hash       → MD5 hash of all column values (used to detect changes later)
  is_active      → 1 = current record, 0 = soft-deleted (for SCD2 later)
  pipeline_phase → "bronze" (which layer this file belongs to)
=============================================================================
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import hashlib
import uuid
import os
import json
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
LOCAL_CSV_FOLDER    = "/home/shreyansh-jain/Documents/Ameriprise_bank_project/phase2_complete_package/phase2_guide/Blob_Vendor_Data"           # Where your CSV files are
OUTPUT_PARQUET_DIR  = "./output_parquet"  # Where Parquet files will be saved locally

# Batch ID: one UUID for this entire pipeline run
# All 4 files get the same batch_id so you can group them together
BATCH_ID    = str(uuid.uuid4())
RUN_DATE    = datetime.utcnow().strftime("%Y-%m-%d")
RUN_TS      = datetime.utcnow().isoformat() + "Z"

# ─────────────────────────────────────────────────────────────
# FILE CONVERSION CONFIGS
# Each dict defines how to convert one source CSV file
# ─────────────────────────────────────────────────────────────
FILE_CONFIGS = [
    {
        "input_csv":     "payment_gateway_logs_1.csv",
        "output_name":   "payment_gateway_logs_historical",
        "s3_zone":       "bronze/payment_gateway_logs",
        "src_system":    "payment_gateway_csv",
        "load_type":     "historical",
        "description":   "Payment Gateway Logs — Historical Jan 2024 to Jan 2025",
        "pk_columns":    ["txn_id"],          # primary key columns for hash
        "dtype_map": {                         # enforce correct data types
            "txn_id":             "int64",
            "gateway_name":       "str",
            "gateway_status":     "str",
            "response_code":      "str",
            "processing_time_ms": "int64",
            "device_type":        "str",
            "geo_location":       "str",
            "processed_timestamp":"str",
        },
        "date_columns":  ["processed_timestamp"],
        "partitions":    ["load_date"],        # partition Parquet by this column
    },
    {
        "input_csv":     "payment_gateway_logs_2_incremental.csv",
        "output_name":   "payment_gateway_logs_incremental",
        "s3_zone":       "bronze/payment_gateway_logs",
        "src_system":    "payment_gateway_csv",
        "load_type":     "incremental",
        "description":   "Payment Gateway Logs — Incremental Feb 2025",
        "pk_columns":    ["txn_id"],
        "dtype_map": {
            "txn_id":             "int64",
            "gateway_name":       "str",
            "gateway_status":     "str",
            "response_code":      "str",
            "processing_time_ms": "int64",
            "device_type":        "str",
            "geo_location":       "str",
            "processed_timestamp":"str",
        },
        "date_columns":  ["processed_timestamp"],
        "partitions":    ["load_date"],
    },
    {
        "input_csv":     "credit_bureau_reports_1.csv",
        "output_name":   "credit_bureau_reports_historical",
        "s3_zone":       "bronze/credit_bureau_reports",
        "src_system":    "credit_bureau_csv",
        "load_type":     "historical",
        "description":   "Credit Bureau Reports — Historical Jan 2025",
        "pk_columns":    ["customer_id", "bureau_pull_date"],
        "dtype_map": {
            "customer_id":              "int64",
            "credit_score":             "int64",
            "risk_grade":               "str",
            "external_active_loans":    "int64",
            "external_overdue_amount":  "float64",
            "bureau_pull_date":         "str",
        },
        "date_columns":  ["bureau_pull_date"],
        "partitions":    ["load_date"],
    },
    {
        "input_csv":     "credit_bureau_reports_2_incremental.csv",
        "output_name":   "credit_bureau_reports_incremental",
        "s3_zone":       "bronze/credit_bureau_reports",
        "src_system":    "credit_bureau_csv",
        "load_type":     "incremental",
        "description":   "Credit Bureau Reports — Incremental Feb 2025",
        "pk_columns":    ["customer_id", "bureau_pull_date"],
        "dtype_map": {
            "customer_id":              "int64",
            "credit_score":             "int64",
            "risk_grade":               "str",
            "external_active_loans":    "int64",
            "external_overdue_amount":  "float64",
            "bureau_pull_date":         "str",
        },
        "date_columns":  ["bureau_pull_date"],
        "partitions":    ["load_date"],
    },
]


def compute_row_hash(row: pd.Series) -> str:
    """
    Compute MD5 hash of all column values in a row.
    This hash changes if ANY column value changes.
    Used later in Silver layer to detect changed records (SCD2).
    
    Example:
      Row: txn_id=30001, gateway_name=Stripe, amount=100
      Hash: a1b2c3d4e5f6...  (unique fingerprint for this exact row)
    """
    concat = "|".join(str(v) for v in row.values)
    return hashlib.md5(concat.encode("utf-8")).hexdigest()


def add_metadata_columns(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    """
    Add 7 pipeline metadata columns to every row.
    These columns exist in EVERY bronze table — they are the audit trail.
    """
    df = df.copy()

    # 1. src_system: where the data came from
    df["src_system"]     = config["src_system"]

    # 2. src_file_name: original filename (traceability)
    df["src_file_name"]  = config["input_csv"]

    # 3. load_timestamp: exact moment this row was loaded
    df["load_timestamp"] = RUN_TS

    # 4. load_date: date portion (used for Parquet partitioning)
    df["load_date"]      = RUN_DATE

    # 5. batch_id: UUID grouping all rows from this pipeline run
    df["batch_id"]       = BATCH_ID

    # 6. row_hash: MD5 fingerprint of all source column values
    #    (computed BEFORE adding metadata cols so hash = source data only)
    source_cols = [c for c in df.columns if c not in
                   ["src_system","src_file_name","load_timestamp","load_date","batch_id"]]
    df["row_hash"] = df[source_cols].apply(compute_row_hash, axis=1)

    # 7. is_active: 1 = current record (0 = deleted, used in SCD2 later)
    df["is_active"]      = 1

    # 8. pipeline_phase: which layer this belongs to
    df["pipeline_phase"] = "bronze"

    return df


def enforce_dtypes(df: pd.DataFrame, dtype_map: dict) -> pd.DataFrame:
    """
    Enforce correct data types on each column.
    CSVs read everything as strings by default — we fix that here.
    """
    for col, dtype in dtype_map.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "str":
                df[col] = df[col].astype(str).str.strip()
            elif dtype == "int64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
            elif dtype == "float64":
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")
        except Exception as e:
            print(f"    [WARN] Could not cast {col} to {dtype}: {e}")
    return df


def validate_no_empty_df(df, filename):
    """Raise an error if the dataframe is empty — this should never happen."""
    if df.empty:
        raise ValueError(f"ERROR: {filename} loaded 0 rows. Check the file path.")


def print_df_stats(df, config):
    """Print a quick stats summary of the converted dataframe."""
    print(f"\n    Shape          : {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"    Memory usage   : {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
    print(f"    Null counts    :")
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0]
    if null_counts.empty:
        print(f"      (no nulls found)")
    else:
        for col, cnt in null_counts.items():
            print(f"      {col}: {cnt:,} nulls")
    print(f"\n    Sample of metadata columns added:")
    meta_cols = ["src_system", "src_file_name", "batch_id", "row_hash", "is_active", "load_date"]
    existing_meta = [c for c in meta_cols if c in df.columns]
    print(df[existing_meta].head(2).to_string(index=False))


def save_as_parquet(df: pd.DataFrame, config: dict, output_dir: str) -> str:
    """
    Save the dataframe as a Parquet file.
    Uses Snappy compression (good balance of speed vs size).
    Returns the output file path.
    """
    out_folder = os.path.join(output_dir, config["s3_zone"])
    os.makedirs(out_folder, exist_ok=True)

    # Include run date in filename for easy identification
    filename  = f"{config['output_name']}_{RUN_DATE}.parquet"
    out_path  = os.path.join(out_folder, filename)

    # Convert to PyArrow table (needed for Parquet with schema metadata)
    table = pa.Table.from_pandas(df, preserve_index=False)

    # Add custom schema metadata (visible when you inspect the Parquet file later)
    custom_meta = {
        "project":         "ameriprise-bank-de-pipeline",
        "source_system":   config["src_system"],
        "load_type":       config["load_type"],
        "batch_id":        BATCH_ID,
        "created_at":      RUN_TS,
        "description":     config["description"],
        "row_count":       str(len(df)),
    }
    merged_meta = {
        **table.schema.metadata,
        b"custom_metadata": json.dumps(custom_meta).encode()
    }
    table = table.replace_schema_metadata(merged_meta)

    # Write with Snappy compression
    pq.write_table(
        table,
        out_path,
        compression="snappy",       # Best for query performance
        use_dictionary=True,        # Compress repeated string values
        write_statistics=True,      # Enables Parquet predicate pushdown
    )

    return out_path


def compare_sizes(csv_path: str, parquet_path: str):
    """Show how much smaller Parquet is compared to CSV."""
    csv_size     = os.path.getsize(csv_path) / 1024 / 1024
    parquet_size = os.path.getsize(parquet_path) / 1024 / 1024
    reduction    = (1 - parquet_size / csv_size) * 100 if csv_size > 0 else 0
    print(f"\n    Size comparison:")
    print(f"      CSV    : {csv_size:.2f} MB")
    print(f"      Parquet: {parquet_size:.2f} MB  ({reduction:.1f}% smaller)")
    print(f"      Saving : {csv_size - parquet_size:.2f} MB per file")


def main():
    print("=" * 65)
    print("  AMERIPRISE BANK — Phase 2 Step 4: CSV → Parquet Conversion")
    print("=" * 65)
    print(f"\n  Batch ID   : {BATCH_ID}")
    print(f"  Run Date   : {RUN_DATE}")
    print(f"  Run Time   : {RUN_TS}")
    print(f"  Output Dir : {OUTPUT_PARQUET_DIR}")

    os.makedirs(OUTPUT_PARQUET_DIR, exist_ok=True)
    results = []

    for config in FILE_CONFIGS:
        csv_path = os.path.join(LOCAL_CSV_FOLDER, config["input_csv"])
        print(f"\n{'─'*65}")
        print(f"  Processing: {config['input_csv']}")
        print(f"  Desc      : {config['description']}")
        print(f"  Load type : {config['load_type'].upper()}")

        if not os.path.exists(csv_path):
            print(f"  [SKIP] File not found: {csv_path}")
            print(f"         Update LOCAL_CSV_FOLDER to the folder with your CSVs")
            results.append({"file": config["input_csv"], "status": "SKIP — not found"})
            continue

        # Step A: Read CSV
        print(f"\n  [A] Reading CSV...")
        df = pd.read_csv(csv_path, dtype=str)   # read all as string first
        print(f"      Raw rows: {len(df):,}  |  Columns: {list(df.columns)}")

        # Step B: Enforce data types
        print(f"\n  [B] Enforcing data types...")
        df = enforce_dtypes(df, config["dtype_map"])
        print(f"      Types enforced for {len(config['dtype_map'])} columns")

        # Step C: Validate
        validate_no_empty_df(df, config["input_csv"])

        # Step D: Add metadata columns
        print(f"\n  [C] Adding metadata columns...")
        df = add_metadata_columns(df, config)
        print(f"      Added: src_system, src_file_name, load_timestamp, load_date,")
        print(f"             batch_id, row_hash, is_active, pipeline_phase")

        # Step E: Stats
        print(f"\n  [D] DataFrame stats:")
        print_df_stats(df, config)

        # Step F: Save as Parquet
        print(f"\n  [E] Saving as Parquet (Snappy compression)...")
        parquet_path = save_as_parquet(df, config, OUTPUT_PARQUET_DIR)
        print(f"      Saved: {parquet_path}")

        # Step G: Size comparison
        compare_sizes(csv_path, parquet_path)

        # Step H: Verify Parquet can be read back
        print(f"\n  [F] Verifying Parquet file (read-back check)...")
        verify_df = pd.read_parquet(parquet_path)
        assert len(verify_df) == len(df), "Row count mismatch!"
        print(f"      Read-back OK: {len(verify_df):,} rows confirmed")

        results.append({
            "file":    config["input_csv"],
            "output":  os.path.basename(parquet_path),
            "rows":    len(df),
            "status":  "CONVERTED"
        })

    # ── Final summary ───────────────────────────────────────────
    print(f"\n{'='*65}")
    print(f"  CONVERSION SUMMARY")
    print(f"{'='*65}")
    for r in results:
        print(f"  [{r['status']:<12}]  {r['file']}")
        if "rows" in r:
            print(f"               → {r.get('output','')}  ({r['rows']:,} rows)")

    # Save batch manifest (a JSON file listing everything in this batch)
    manifest = {
        "batch_id":    BATCH_ID,
        "run_date":    RUN_DATE,
        "run_ts":      RUN_TS,
        "files":       results,
        "total_files": len(results),
    }
    manifest_path = os.path.join(OUTPUT_PARQUET_DIR, f"batch_manifest_{RUN_DATE}.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"\n  Batch manifest saved: {manifest_path}")
    print(f"  (This JSON tracks every file in this batch run)")
    print(f"\n  NEXT STEP: Run step5_upload_parquet_to_s3.py")
    print("=" * 65)


if __name__ == "__main__":
    main()
