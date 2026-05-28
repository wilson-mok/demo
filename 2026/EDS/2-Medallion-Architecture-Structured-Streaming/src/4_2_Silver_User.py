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
dataset = "user"
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
        print(f"  • {silver_table} - {str(e)}")

    # Delete all data in the DQ Table for the silver_table
    spark.sql(f"DELETE FROM {dq_table} WHERE table_name = '{silver_table}'")
    print(f"  ✓ Deleted DQ records for {silver_table}")

    print("=== Reset complete ===\n")

# COMMAND ----------

# DBTITLE 1,Read Bronze Stream
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lower, current_timestamp, to_date, split, size, trim

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

# DBTITLE 1,Apply Transformations
# Apply user transformations
transformed_stream = (bronze_stream
    # Note: We no longer filter out rescued data here - it's handled in foreachBatch
    # This allows us to capture rescued records for the DQ table
    
    # 1. Convert yelping_since to date only and rename to member_since
    .withColumn("member_since", to_date(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))
    
    # 2. Parse elite field into array (handle empty strings)
    .withColumn(
        "elite_years",
        when(trim(col("elite")) == "", F.lit(None))
        .otherwise(split(trim(col("elite")), ",\\s*"))
    )
    
    # 3. Parse friends field into array (handle empty strings)
    .withColumn(
        "friends_array",
        when(trim(col("friends")) == "", F.lit(None))
        .otherwise(split(trim(col("friends")), ",\\s*"))
    )
    
    # 4. Rename review metrics for clarity
    .withColumnRenamed("average_stars", "lifetime_avg_stars")
    .withColumnRenamed("review_count", "lifetime_review_count")
    
    # 5. Add Silver layer metadata
    .withColumn("_bronze_pipeline_run_id", col("_pipeline_run_id"))  # Copy from Bronze
    .withColumn("_bronze_processing_date", col("_processing_date"))  # Copy from Bronze
    .withColumn("_pipeline_run_id", F.lit(_pipeline_run_id))  # New Silver run ID
    .withColumn("_processing_date", current_timestamp())  # New Silver timestamp
    
    # Drop original columns that have been transformed
    .drop("yelping_since", "elite", "friends", "_input_filename", "_input_file_modification_date")
)

print("Transformations applied:")
print("  ✓ Converted yelping_since → member_since (date only)")
print("  ✓ Parsed elite → elite_years (array)")
print("  ✓ Parsed friends → friends_array (array)")
print("  ✓ Renamed: stars → lifetime_avg_stars, review_count → lifetime_review_count")
print("  ✓ Added Bronze lineage metadata")
print("  ✓ Added Silver processing metadata")
print("  ⚠ Rescued data will be split and routed in the write step")

# COMMAND ----------

# DBTITLE 1,Deduplication & Upsert to Silver
from delta.tables import DeltaTable

def write_to_silver(clean_df, batchId):
    """
    Deduplicate and upsert clean records to the Silver table.
    
    Args:
        clean_df: DataFrame containing records without _rescued_data
        batchId: The micro-batch identifier
    
    Returns:
        Number of records processed
    """
    if clean_df.count() == 0:
        print(f"  → Batch {batchId}: No clean records to process")
        return 0
    
    # Check if Silver table exists
    if spark.catalog.tableExists(silver_table):
        # Merge into existing table
        silver_delta = DeltaTable.forName(spark, silver_table)
        
        (silver_delta.alias("target")
            .merge(
                clean_df.alias("source"),
                "target.user_id = source.user_id"
            )
            .whenMatchedUpdateAll()  # Update if exists
            .whenNotMatchedInsertAll()  # Insert if new
            .execute()
        )
    else:
        # Create table on first batch
        clean_df.write.format("delta").mode("append").saveAsTable(silver_table)
    
    deduped_count = clean_df.count()
    return deduped_count

def write_to_quarantine(quarantine_df, batchId):
    """
    Write failed dq records to the DQ table.
    
    Args:
        quarantine_df: DataFrame containing records with dq issues
        batchId: The micro-batch identifier
    Returns:
        int: Number of records quarantined
    """
    if quarantine_df.count() == 0:
        return 0
    
    dq_df = (quarantine_df
        .select(
            F.lit("user").alias("dataset"),
            F.lit(silver_table).alias("table_name"),
            F.lit("silver").alias("layer"),
            F.create_map(
                F.lit("user_id"), col("user_id")
            ).alias("record_key"),
            F.lit("SCHEMA_MISMATCH").alias("dq_category"),
            F.lit("Schema mismatch detected - data captured in rescued_data column").alias("dq_reason"),
            F.array().cast("array<string>").alias("failed_columns"),
            col("_rescued_data").alias("original_record"),
            F.lit(_pipeline_run_id).alias("pipeline_run_id"),
            current_timestamp().alias("quarantine_timestamp"),
            F.lit(False).alias("is_resolved"),
            F.lit(None).cast("timestamp").alias("resolved_timestamp"),
            F.lit(None).cast("string").alias("resolution_notes")
        )
    )
    
    # Write to dq table (append mode)
    dq_df.write.format("delta").mode("append").saveAsTable(dq_table)

    return dq_df.count()
    
print("Functions defined:")
print("  ✓ write_to_silver() - MERGE on key (user_id)")
print("  ✓ write_to_quarantine() - Append with dq records matching DQ table schema")

# COMMAND ----------

# DBTITLE 1,Deduplication & Upsert to Silver

from pyspark.sql.window import Window

def process_micro_batch(microBatchDF, batchId):
    """
    Main orchestration function for each micro-batch:
    1. Split records into clean vs. rescued data
    2. Route rescued data to DQ table
    3. Route clean data to Silver table with deduplication
    """
    
    # Deduplicate within micro-batch - keep latest record per user_id
    window_spec = Window.partitionBy("user_id").orderBy(col("_bronze_processing_date").desc())
    
    deduplicated_df = (microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Split clean vs. dq data
    clean_df = deduplicated_df.filter(col("_rescued_data").isNull())
    quarantine_df = deduplicated_df.filter(col("_rescued_data").isNotNull())
    
    # Route to appropriate destinations
    pass_count = write_to_silver(clean_df, batchId)
    fail_count = write_to_quarantine(quarantine_df, batchId)
    
    print(f"Batch {batchId}: {pass_count} records to Silver, {fail_count} quarantined")

# Write stream using foreachBatch for deduplication and DQ routing
query = (transformed_stream
    .writeStream
    .foreachBatch(process_micro_batch)
    .trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .outputMode("update")
    .queryName("stream-yelp_silver_user")
    .start()
)

print(f"\nStream started:")
print(f"  • Clean records → {silver_table}")
print(f"  • Rescued data → {dq_table}")

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
# MAGIC     
# MAGIC -- Verify the Silver table
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_users,
# MAGIC   AVG(lifetime_review_count) AS avg_review_count,
# MAGIC   MIN(lifetime_avg_stars) AS min_stars,
# MAGIC   MAX(lifetime_avg_stars) AS max_stars,
# MAGIC   MIN(member_since) AS earliest_member_since,
# MAGIC   MAX(member_since) AS latest_member_since,
# MAGIC   COUNT(DISTINCT user_id) AS unique_users
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_silver_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT user_id, COUNT(*) 
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_silver_user 
# MAGIC GROUP BY user_id
# MAGIC HAVING COUNT(*) > 1;