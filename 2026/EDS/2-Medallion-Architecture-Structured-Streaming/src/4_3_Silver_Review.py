# Databricks notebook source
# DBTITLE 1,Parameters & Configuration
# Parameters
dbutils.widgets.text("pipeline_run_id", "default_silver_run", "Pipeline Run ID")
_pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

dbutils.widgets.dropdown("reset_before_run", "false", ["true", "false"], "Reset Before Run")
reset_before_run = dbutils.widgets.get("reset_before_run")

# Configuration
catalog = "demo_catalog"
schema = "demo_yelp_academic"
src_table_prefix = "ss_bronze_"
target_table_prefix = "ss_silver_"
dataset = "review"
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

    # 1. Convert date string to date and timestamp using try_to_date for tolerance
    .withColumn("review_date", F.to_date(col("date"), "yyyy-MM-dd HH:mm:ss"))
    
    # 2. Rename columns for clarity
    .withColumnRenamed("useful", "review_useful_votes")
    .withColumnRenamed("funny", "review_funny_votes")
    .withColumnRenamed("cool", "review_cool_votes")
    .withColumnRenamed("text", "review_text")
    
    # 3. Add quality check flags
    .withColumn("dq_primary_key_valid", col("review_id").isNotNull())
    .withColumn("dq_user_id_valid", col("user_id").isNotNull())
    .withColumn("dq_business_id_valid", col("business_id").isNotNull())
    .withColumn("dq_date_valid", col("review_date").isNotNull())
    .withColumn("dq_stars_valid", col("stars").isNotNull() & col("stars").between(1, 5))
    .withColumn("dq_schema_valid", col("_rescued_data").isNull())
    
    # 4. Determine overall quality status
    .withColumn("dq_pass",
        col("dq_primary_key_valid") &
        col("dq_user_id_valid") &
        col("dq_business_id_valid") &
        col("dq_date_valid") &
        col("dq_stars_valid") &
        col("dq_schema_valid")
    )
    
    # 5. Add lineage metadata
    .withColumn("_bronze_pipeline_run_id", col("_pipeline_run_id"))
    .withColumn("_bronze_processing_date", col("_processing_date"))
    .withColumn("_pipeline_run_id", F.lit(_pipeline_run_id))
    .withColumn("_processing_date", current_timestamp())
    
    # Rename processing metadata for Silver
    .withColumnRenamed("_pipeline_run_id", "_silver_pipeline_run_id")
    .withColumnRenamed("_processing_date", "_silver_processing_date")
    
    # Drop original Bronze metadata columns
    .drop("date", "_input_filename", "_input_file_modification_date")
)

print("Transformations applied:")
print("  ✓ Date conversions: date → review_date (using try_to_date for tolerance)")
print("  ✓ Column renaming for clarity")
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
    
    Args:
        pass_df: DataFrame containing records that passed quality checks
        batchId: The micro-batch identifier
    
    Returns:
        int: Number of records written
    """
    if pass_df.count() == 0:
        return 0
    
    # Select only the columns needed for Silver
    silver_columns = [
        "review_id", "user_id", "business_id",
        "review_date", "stars",
        "review_useful_votes", "review_funny_votes", "review_cool_votes",
        "review_text",
        "_bronze_pipeline_run_id", "_bronze_processing_date",
        "_silver_pipeline_run_id", "_silver_processing_date"
    ]
    
    pass_df_select = pass_df.select(*silver_columns)
    
    # Upsert to Silver table
    if spark.catalog.tableExists(silver_table):
        silver_delta = DeltaTable.forName(spark, silver_table)
        (silver_delta.alias("target")
            .merge(
                pass_df_select.alias("source"),
                "target.review_id = source.review_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        pass_df_select.write.format("delta").mode("append").saveAsTable(silver_table)
    
    return pass_df_select.count()


def write_to_quarantine(quarantine_df, batchId):
    """
    Write failed records to quarantine table with detailed reasons.
    Handles both schema mismatches (rescued data) and business rule violations.
    
    Args:
        quarantine_df: DataFrame containing records that failed quality checks
        batchId: The micro-batch identifier
    
    Returns:
        int: Number of records quarantined
    """
    if quarantine_df.count() == 0:
        return 0
    
    # Determine category and reason based on failure type
    quarantine_df = (quarantine_df
        .withColumn("dq_category",
            when(~col("dq_schema_valid"), F.lit("SCHEMA_MISMATCH"))
            .when(~col("dq_primary_key_valid"), F.lit("MISSING_PRIMARY_KEY"))
            .when(~col("dq_user_id_valid") | ~col("dq_business_id_valid"), F.lit("MISSING_FOREIGN_KEY"))
            .when(~col("dq_stars_valid"), F.lit("INVALID_VALUE"))
            .otherwise(F.lit("BUSINESS_RULE"))
        )
        .withColumn("dq_reasons_array", F.array_remove(F.array(
            when(~col("dq_primary_key_valid"), F.lit("Missing primary key: review_id")).otherwise(F.lit(None)),
            when(~col("dq_user_id_valid"), F.lit("Missing foreign key: user_id")).otherwise(F.lit(None)),
            when(~col("dq_business_id_valid"), F.lit("Missing foreign key: business_id")).otherwise(F.lit(None)),
            when(~col("dq_date_valid"), F.lit("Invalid date format")).otherwise(F.lit(None)),
            when(~col("dq_stars_valid"), F.lit("Invalid star rating: must be 1-5")).otherwise(F.lit(None)),
            when(~col("dq_schema_valid"), F.lit("Schema mismatch: rescued data present")).otherwise(F.lit(None))
        ), None))
        .withColumn("dq_reason", F.array_join(col("dq_reasons_array"), "; "))
        .withColumn("failed_columns_array", F.array_remove(F.array(
            when(~col("dq_primary_key_valid"), F.lit("review_id")).otherwise(F.lit(None)),
            when(~col("dq_user_id_valid"), F.lit("user_id")).otherwise(F.lit(None)),
            when(~col("dq_business_id_valid"), F.lit("business_id")).otherwise(F.lit(None)),
            when(~col("dq_date_valid"), F.lit("review_date")).otherwise(F.lit(None)),
            when(~col("dq_stars_valid"), F.lit("stars")).otherwise(F.lit(None)),
            when(~col("dq_schema_valid"), F.lit("_rescued_data")).otherwise(F.lit(None))
        ), None))
        .select(
            F.lit("review").alias("dataset"),
            F.lit(quarantine_df).alias("table_name"),
            F.lit("silver").alias("layer"),
            F.create_map(
                F.lit("review_id"), col("review_id")
            ).alias("record_key"),
            col("dq_category"),
            col("dq_reason"),
            col("failed_columns_array").alias("failed_columns"),
            F.coalesce(
                col("_rescued_data"),
                F.to_json(F.struct("*"))
            ).alias("original_record"),
            col("_silver_pipeline_run_id").alias("pipeline_run_id"),
            col("_silver_processing_date").alias("quarantine_timestamp"),
            F.lit(False).alias("is_resolved"),
            F.lit(None).cast("timestamp").alias("resolved_timestamp"),
            F.lit(None).cast("string").alias("resolution_notes")
        )
    )
    
    # Write to dq table (append mode)
    quarantine_df.write.format("delta").mode("append").saveAsTable(dq_table)
    
    return quarantine_df.count()

print("Functions defined:")
print("  ✓ write_to_silver() - MERGE on key (review_id)")
print("  ✓ write_to_quarantine() - Append with detailed failure reasons matching DQ schema")

# COMMAND ----------

# DBTITLE 1,Deduplication & Upsert to Silver
def process_microbatch(microBatchDF, batchId):
    """
    Process micro-batch:
    1. Deduplicate within micro-batch (keep latest by _bronze_processing_date)
    2. Split into schema-valid vs. schema-invalid (rescued data)
    3. For schema-valid records, split into pass/fail based on business rule quality checks
    4. Write pass records to Silver (upsert)
    5. Write both schema-invalid and business-rule-fail records to Quarantine with reasons
    """
    
    # Deduplicate within micro-batch - keep latest record per review_id
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("review_id").orderBy(col("_bronze_processing_date").desc())
    
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
    .queryName("stream-yelp_silver_review")
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
# MAGIC     COUNT(*) as total_records,
# MAGIC     MIN(review_date) as earliest_review,
# MAGIC     MAX(review_date) as latest_review,
# MAGIC     ROUND(AVG(stars), 2) as avg_rating,
# MAGIC     SUM(review_useful_votes) as total_useful_votes,
# MAGIC     SUM(review_funny_votes) as total_funny_votes,
# MAGIC     SUM(review_cool_votes) as total_cool_votes
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_silver_review

# COMMAND ----------

# DBTITLE 1,Sample Quarantine Records
# MAGIC %sql
# MAGIC -- Sample quarantined records for investigation
# MAGIC -- If error means this table is not created due to no error. 
# MAGIC
# MAGIC SELECT count(*)
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_dq_common_issues
# MAGIC WHERE table_name = 'demo_catalog.demo_yelp_academic.ss_silver_review';