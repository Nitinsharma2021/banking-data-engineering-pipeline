# PHASE 2 — S3 Data Lake Setup (Complete Guide)
## Ameriprise Bank Data Engineering Project

### Files in this package:
1. requirements.txt                ← Install these first
2. step1_verify_aws_setup.py       ← Verify AWS CLI and boto3 are working  
3. step2_create_s3_bucket.py       ← Create S3 bucket + all zone folders
4. step3_upload_csv_files.py       ← Upload your 4 CSV source files
5. step4_csv_to_parquet.py         ← Convert CSVs → Parquet (with metadata cols)
6. step5_upload_parquet_to_s3.py   ← Upload Parquet files to bronze/ zone
7. step6_verify_s3_structure.py    ← Verify everything is correctly in S3
8. step7_create_metadata_files.py  ← Create watermark + catalog tracking files

### Run order:
pip install -r requirements.txt
python step1_verify_aws_setup.py
python step2_create_s3_bucket.py
python step3_upload_csv_files.py
python step4_csv_to_parquet.py
python step5_upload_parquet_to_s3.py
python step6_verify_s3_structure.py
python step7_create_metadata_files.py
