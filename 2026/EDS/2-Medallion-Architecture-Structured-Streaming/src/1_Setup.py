# Databricks notebook source
# DBTITLE 1,Create Volume Subfolders
# Base volume path
base_path = "/Volumes/demo_catalog/demo_yelp_academic/raw_data"

# Subfolder for each Yelp datasets
subfolders = ["business", "checkin", "review", "tip", "user"]

# Create the subfolders
for folder in subfolders:
    folder_path = f"{base_path}/{folder}"
    try:
        dbutils.fs.mkdirs(folder_path)
        print(f"✓ Created: {folder_path}")
    except Exception as e:
        print(f"✗ Error creating {folder_path}: {str(e)}")

# List all folders to verify
print("\n=== Current folder structure ===")
folders = dbutils.fs.ls(base_path)
for folder in folders:
    print(f"  {folder.path}")

# COMMAND ----------

# Move each JSON file into its corresponding subfolder
# 
# Note: 
# - If you are continuing from the 'Trusted Data Assistant' demo, run the move commands below. 
# - If you are starting from scratch, Upload the file to the proper folders.

dbutils.fs.mv(f"{base_path}/yelp_academic_dataset_business.json", f"{base_path}/business/")
dbutils.fs.mv(f"{base_path}/yelp_academic_dataset_checkin.json", f"{base_path}/checkin/")
dbutils.fs.mv(f"{base_path}/yelp_academic_dataset_review.json", f"{base_path}/review/")
dbutils.fs.mv(f"{base_path}/yelp_academic_dataset_tip.json", f"{base_path}/tip/")
dbutils.fs.mv(f"{base_path}/yelp_academic_dataset_user.json", f"{base_path}/user/")


# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %sql 
# MAGIC -- Crete a Volume 'checkpoints' dedicated to AutoLoader and Structured Streaming checkpoint files
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS demo_catalog.demo_yelp_academic.checkpoints;

# COMMAND ----------

# DBTITLE 1,Create Common DQ Issues Table
# MAGIC %sql
# MAGIC -- Create a common Data Quality issues table for all silver layer pipelines
# MAGIC -- This centralizes quality monitoring across all datasets
# MAGIC
# MAGIC CREATE OR REPLACE TABLE demo_catalog.demo_yelp_academic.ss_dq_common_issues (
# MAGIC   -- Source identification
# MAGIC   dataset STRING COMMENT 'Dataset name (business, tip, review, user, checkin)',
# MAGIC   table_name STRING COMMENT 'Fully qualified source table name',
# MAGIC   layer STRING COMMENT 'Medallion layer (bronze, silver, gold)',
# MAGIC   
# MAGIC   -- Record identification (composite key as JSON for flexibility)
# MAGIC   record_key MAP<STRING, STRING> COMMENT 'Key-value pairs identifying the record',
# MAGIC   
# MAGIC   -- Quality issue details
# MAGIC   dq_category STRING COMMENT 'Issue category (MISSING_KEY, INVALID_DATE, MISSING_CONTENT, SCHEMA_MISMATCH, etc)',
# MAGIC   dq_reason STRING COMMENT 'Detailed failure reason',
# MAGIC   failed_columns ARRAY<STRING> COMMENT 'List of columns that failed validation',
# MAGIC   
# MAGIC   -- Original data (for investigation)
# MAGIC   original_record STRING COMMENT 'JSON string of the original record',
# MAGIC   
# MAGIC   -- Lineage and tracking
# MAGIC   pipeline_run_id STRING COMMENT 'Pipeline run that captured this issue',
# MAGIC   quarantine_timestamp TIMESTAMP COMMENT 'When the record was quarantined',
# MAGIC   
# MAGIC   -- Resolution tracking
# MAGIC   is_resolved BOOLEAN COMMENT 'Whether the issue has been resolved',
# MAGIC   resolved_timestamp TIMESTAMP COMMENT 'When the issue was resolved',
# MAGIC   resolution_notes STRING COMMENT 'How the issue was resolved'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Common data quality issues table for all silver layer pipelines';