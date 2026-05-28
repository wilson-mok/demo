# Databricks notebook source
# DBTITLE 1,Parameters & Configuration
dbutils.widgets.text("pipeline_run_id", "default_silver_run", "Pipeline Run ID")
_pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

dbutils.widgets.dropdown("reset_before_run", "false", ["true", "false"], "Reset Before Run")
reset_before_run = dbutils.widgets.get("reset_before_run")

# Configuration
catalog = "demo_catalog"
schema = "demo_yelp_academic"
src_table_prefix = "ss_bronze_"
target_table_prefix = "ss_silver_"
dataset = "tip"
dq_dataset = "ss_dq_common_issues"

# Table names
bronze_table = f"{catalog}.{schema}.{src_table_prefix}{dataset}"
silver_table = f"{catalog}.{schema}.{target_table_prefix}{dataset}"

# DQ table configuration
dq_table = f"{catalog}.{schema}.{dq_dataset}"

# Checkpoint location
checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/{target_table_prefix}{dataset}/"

print(f"Pipeline Run ID: {_pipeline_run_id}\n")
print(f"Bronze Table: {bronze_table}")
print(f"Silver Table: {silver_table}")
print(f"DQ Table: {dq_table}\n")
print(f"Checkpoint Path: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,Reset data (Demo only)
# ** For Demo or Development/Testing only **
#   Auto cleanup if reset flag is set

print(f"Reset Before Run: {reset_before_run}")
if reset_before_run == "true":
    print("\n=== RESETTING: Dropping tables and checkpoints ===")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"  ✓ Reset {silver_table}")
    except Exception as e:
        print(f"  • Error during reset - {str(e)}")

    # Delete all data in the DQ Table for the silver_table
    spark.sql(f"DELETE FROM {dq_table} WHERE table_name = '{silver_table}'")
    print(f"  ✓ Deleted DQ records for {silver_table}")

    print("=== Reset complete ===\n")

# COMMAND ----------

# DBTITLE 1,Read Bronze Stream
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, current_timestamp
from delta.tables import DeltaTable

# Read streaming data from Bronze
bronze_stream = (spark.readStream
    .format("delta")
    .option("mergeSchema", "false")  # Fail if schema changes
    .option("rescuedDataColumn", "_rescued_data")  # Capture schema mismatches
    .table(bronze_table)
)

print(f"Reading stream from: {bronze_table}")
print(f"Schema validation: Strict (mergeSchema=false)")
print(f"Rescued data column: _rescued_data")

# COMMAND ----------

# DBTITLE 1,Apply Transformation
# Apply business logic transformations
transformed_stream = (bronze_stream
    # Note: We no longer filter out rescued data here - it's handled in foreachBatch
    # This allows us to capture rescued records for the DQ table

    # 1. Convert date string to date
    .withColumn("tip_date", F.to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))
    
    # 2. Handle compliment_count nulls as 0 (no DQ flag needed)
    .withColumn("tip_compliment_count", F.coalesce(col("compliment_count"), F.lit(0)))
    
    # 3. Rename columns for clarity
    .withColumnRenamed("text", "tip_text")
    
    # 4. Add quality check flags
    .withColumn("dq_user_id_valid", col("user_id").isNotNull())
    .withColumn("dq_business_id_valid", col("business_id").isNotNull())
    .withColumn("dq_date_valid", col("tip_date").isNotNull())
    .withColumn("dq_text_valid", col("tip_text").isNotNull())
    
    # 5. Determine overall quality status
    .withColumn("dq_pass",
        col("dq_user_id_valid") &
        col("dq_business_id_valid") &
        col("dq_date_valid") &
        col("dq_text_valid")
    )
    
    # 6. Add lineage metadata
    .withColumn("_bronze_pipeline_run_id", col("_pipeline_run_id"))
    .withColumn("_bronze_processing_date", col("_processing_date"))
    .withColumn("_pipeline_run_id", F.lit(_pipeline_run_id))
    .withColumn("_processing_date", current_timestamp())
    
    # Drop original Bronze metadata columns
    .drop("date", "compliment_count", "_input_filename", "_input_file_modification_date")
)

print("Transformations applied:")
print("  ✓ Date conversion: date → tip_date")
print("  ✓ Compliment count: nulls treated as 0")
print("  ✓ Column renaming: text → tip_text")
print("  ✓ Quality validation flags added")
print("  ✓ Bronze lineage metadata preserved")
print("  ✓ Silver processing metadata added")
print("  ⚠ Rescued data will be split and routed in the write step")

# COMMAND ----------

# DBTITLE 1,Functions for handling of passing and quarantine records
from delta.tables import DeltaTable

def write_to_silver(pass_df, batchId):
    """
    Write passing records to Silver table using upsert pattern.
    Composite key: (user_id, business_id, tip_date)
    
    Args:
        pass_df: DataFrame containing records that passed quality checks
        batchId: The micro-batch identifier
    
    Returns:
        int: Number of records written
    """
    if pass_df.count() == 0:
        return 0
    
    # Select only the columns needed for Silver (no _rescued_data or dq flags)
    silver_columns = [
        "user_id", "business_id", "tip_date",
        "tip_text", "tip_compliment_count",
        "_bronze_pipeline_run_id", "_bronze_processing_date",
        "_pipeline_run_id", "_processing_date"
    ]
    
    silver_df = pass_df.select(*silver_columns)
    
    # Check if Silver table exists
    if not spark.catalog.tableExists(silver_table):
        # Create the table on first write
        silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return silver_df.count()
    
    # MERGE for upsert using composite key
    silver_delta = DeltaTable.forName(spark, silver_table)
    
    silver_delta.alias("target").merge(
        silver_df.alias("source"),
        "target.user_id = source.user_id AND target.business_id = source.business_id AND target.tip_date = source.tip_date"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()
    
    return silver_df.count()


def write_to_quarantine(quarantine_df, batchId):
    """
    Write failing records to Quarantine table with failure reasons.
    Handles both schema mismatches (rescued data) and business rule violations.
    
    Args:
        quarantine_df: DataFrame containing records that failed quality checks
        batchId: The micro-batch identifier
    
    Returns:
        int: Number of records quarantined
    """
    if quarantine_df.count() == 0:
        return 0
    
    # Build quarantine record with proper schema
    quarantine_df = (quarantine_df
        .withColumn("dq_category",
            when(col("_rescued_data").isNotNull(), F.lit("SCHEMA_MISMATCH"))
            .when(~col("dq_user_id_valid") | ~col("dq_business_id_valid"), F.lit("MISSING_KEY"))
            .when(~col("dq_date_valid"), F.lit("INVALID_DATE"))
            .when(~col("dq_text_valid"), F.lit("MISSING_CONTENT"))
            .otherwise(F.lit("OTHER"))
        )
        .withColumn("dq_reasons_array", F.array_remove(F.array(
            when(~col("dq_user_id_valid"), F.lit("Missing user_id")).otherwise(F.lit(None)),
            when(~col("dq_business_id_valid"), F.lit("Missing business_id")).otherwise(F.lit(None)),
            when(~col("dq_date_valid"), F.lit("Invalid date")).otherwise(F.lit(None)),
            when(~col("dq_text_valid"), F.lit("Missing tip text")).otherwise(F.lit(None)),
            when(col("_rescued_data").isNotNull(), F.lit("Schema mismatch detected")).otherwise(F.lit(None))
        ), None))
        .withColumn("dq_reason", F.array_join(col("dq_reasons_array"), "; "))
        .withColumn("failed_columns_array", F.array_remove(F.array(
            when(~col("dq_user_id_valid"), F.lit("user_id")).otherwise(F.lit(None)),
            when(~col("dq_business_id_valid"), F.lit("business_id")).otherwise(F.lit(None)),
            when(~col("dq_date_valid"), F.lit("tip_date")).otherwise(F.lit(None)),
            when(~col("dq_text_valid"), F.lit("tip_text")).otherwise(F.lit(None)),
            when(col("_rescued_data").isNotNull(), F.lit("_rescued_data")).otherwise(F.lit(None))
        ), None))
        .select(
            F.lit("tip").alias("dataset"),
            F.lit(quarantine_df).alias("table_name"),
            F.lit("silver").alias("layer"),
            F.create_map(
                F.lit("user_id"), col("user_id"),
                F.lit("business_id"), col("business_id"),
                F.lit("tip_date"), col("tip_date").cast("string")
            ).alias("record_key"),
            col("dq_category"),
            col("dq_reason"),
            col("failed_columns_array").alias("failed_columns"),
            F.coalesce(
                col("_rescued_data"),
                F.to_json(F.struct("*"))
            ).alias("original_record"),
            col("_pipeline_run_id").alias("pipeline_run_id"),
            col("_processing_date").alias("quarantine_timestamp"),
            F.lit(False).alias("is_resolved"),
            F.lit(None).cast("timestamp").alias("resolved_timestamp"),
            F.lit(None).cast("string").alias("resolution_notes")
        )
    )
    
    # Write to quarantine table (append mode)
    quarantine_df.write.format("delta").mode("append").saveAsTable(dq_table)
    
    return quarantine_df.count()


print("Functions defined:")
print("  ✓ write_to_silver() - MERGE on composite key (user_id, business_id, tip_date)")
print("  ✓ write_to_quarantine() - Append with detailed failure reasons matching DQ schema")

# COMMAND ----------

# DBTITLE 1,Deduplication & Upsert to Silver
def process_microbatch(microBatchDF, batchId):
    """
    Process micro-batch:
    1. Deduplicate within micro-batch on composite key (user_id, business_id, tip_date)
       - Keep latest by _bronze_processing_date
    2. Split into schema-valid vs. schema-invalid (rescued data)
    3. For schema-valid records, split into pass/fail based on business rule quality checks
    4. Write pass records to Silver (upsert on composite key)
    5. Write both schema-invalid and business-rule-fail records to Quarantine with reasons
    """
    
    # Deduplicate within micro-batch - keep latest record per composite key
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("user_id", "business_id", "tip_date").orderBy(col("_bronze_processing_date").desc())
    
    deduplicated_df = (microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    # First split: schema mismatches (rescued data) vs. schema-valid
    schema_valid_df = deduplicated_df.filter(col("_rescued_data").isNull())
    schema_invalid_df = deduplicated_df.filter(col("_rescued_data").isNotNull())
    
    
    # Second split (on schema-valid records): business rule pass vs. fail
    pass_df = schema_valid_df.filter(col("dq_pass") == True)
    business_rule_fail_df = schema_valid_df.filter(col("dq_pass") == False)
    
    # Write to appropriate destinations
    pass_count = write_to_silver(pass_df, batchId)
    schema_fail_count = write_to_quarantine(schema_invalid_df, batchId)
    business_fail_count = write_to_quarantine(business_rule_fail_df, batchId)
    
    total_fail = schema_fail_count + business_fail_count
    print(f"Batch {batchId}: {pass_count} records to Silver, {total_fail} quarantined ({schema_fail_count} schema, {business_fail_count} business rules)")

# Write stream using foreachBatch
query = (transformed_stream
    .writeStream
    .foreachBatch(process_microbatch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("update")
    .queryName("stream-yelp_silver_tip")
    .start()
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream (Demo only)
# This step is for demo only. 
#  - In Databricks Jobs, you do not need to use awaitTermination() because the job automatically waits for all streaming queries to finish before exiting.

import time

# Stream info
print(f"Streaming query started: {query.name}")
print(f"Query ID: {query.id}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target:")
print(f"  • Clean records → {silver_table}")
print(f"  • DQ failures → {dq_table}")
print("\nDeduplication: Latest record per review_id (by _bronze_processing_date)")
print("Write mode: MERGE (upsert pattern) for Silver, APPEND for Quarantine")
print("Quality filtering: Schema mismatches and business rule violations routed to Quarantine")

print("\nWaiting for stream to process all available data...")

query.awaitTermination()

print("\n✓ Stream completed successfully")

# COMMAND ----------

# DBTITLE 1,Verify Silver Table
# MAGIC %sql
# MAGIC -- Verify the Silver table
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_tips,
# MAGIC     MIN(tip_date) as earliest_tip,
# MAGIC     MAX(tip_date) as latest_tip,
# MAGIC     COUNT(DISTINCT user_id) as unique_users,
# MAGIC     COUNT(DISTINCT business_id) as unique_businesses,
# MAGIC     SUM(tip_compliment_count) as total_compliments,
# MAGIC     ROUND(AVG(tip_compliment_count), 2) as avg_compliments_per_tip
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_silver_tip