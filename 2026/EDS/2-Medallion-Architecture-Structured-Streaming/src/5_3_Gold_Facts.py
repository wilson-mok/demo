# Databricks notebook source
# DBTITLE 1,Parameters & Configuration
# Parameters
dbutils.widgets.text("pipeline_run_id", "default_gold_run", "Pipeline Run ID")
_pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

dbutils.widgets.dropdown("reset_before_run", "false", ["true", "false"], "Reset Before Run")
reset_before_run = dbutils.widgets.get("reset_before_run")

# Configuration
catalog = "demo_catalog"
schema = "demo_yelp_academic"
silver_prefix = "ss_silver_"
gold_prefix = "ss_gold_"
dq_dataset = "ss_dq_common_issues"

# Source tables
review_silver = f"{catalog}.{schema}.{silver_prefix}review"

# Dimension tables (for lookups)
dim_business = f"{catalog}.{schema}.{gold_prefix}dim_business"
dim_user = f"{catalog}.{schema}.{gold_prefix}dim_user"
dim_date = f"{catalog}.{schema}.{gold_prefix}dim_date"

# Target fact table
fact_review = f"{catalog}.{schema}.{gold_prefix}fact_review"

# DQ table configuration
dq_table = f"{catalog}.{schema}.{dq_dataset}"

# Checkpoint location
checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/{gold_prefix}fact_review/"

print(f"Pipeline Run ID: {_pipeline_run_id}")
print(f"\nSource: {review_silver}")
print(f"\nDimensions for Lookup:")
print(f"  Business: {dim_business}")
print(f"  User: {dim_user}")
print(f"  Date: {dim_date}")
print(f"\nTarget Fact: {fact_review}")
print(f"DQ Issues: {dq_table}")
print(f"Checkpoint: {checkpoint_path}")

# COMMAND ----------

# DBTITLE 1,Reset Data (Demo only)
# ** For Demo or Development/Testing only **
#   Drop tables and checkpoints if reset flag is set

print(f"Reset Before Run: {reset_before_run}")
if reset_before_run == "true":
    print("\n=== RESETTING: Dropping tables and checkpoints ===")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {fact_review}")
        dbutils.fs.rm(checkpoint_path, recurse=True)
        print(f"  ✓ Reset {fact_review}")
    except Exception as e:
        print(f"  • Error: {str(e)}")


    # Delete all data in the DQ Table for the silver_table
    spark.sql(f"DELETE FROM {dq_table} WHERE table_name = '{fact_review}'")
    print(f"  ✓ Deleted DQ records for {fact_review}")
    
    print("=== Reset complete ===\n")
else:
    print("Skipping reset")

# COMMAND ----------

# DBTITLE 1,Load Dimension Tables for Lookup
from pyspark.sql import functions as F
from pyspark.sql.functions import col, broadcast, current_timestamp
from delta.tables import DeltaTable

# Load dimensions as static DataFrames for stream-static joins
print("Loading dimension tables for lookup...")

# Business dimension
dim_business_df = spark.table(dim_business).select(
    col("SK_business_id"),
    col("NK_business_id")
)
print(f"  ✓ Loaded {dim_business_df.count():,} businesses")

# User dimension
dim_user_df = spark.table(dim_user).select(
    col("SK_user_id"),
    col("NK_user_id")
)
print(f"  ✓ Loaded {dim_user_df.count():,} users")

# Date dimension
dim_date_df = spark.table(dim_date).select(
    col("SK_date_id"),
    col("date_actual")
)
print(f"  ✓ Loaded {dim_date_df.count():,} dates")

print("\n✓ All dimensions loaded for stream-static joins")

# COMMAND ----------

# DBTITLE 1,Read Silver Review Stream
# Read streaming from Silver Review
review_stream = (spark.readStream
    .format("delta")
    .option("mergeSchema", "false")
    .table(review_silver)
)

print(f"Reading stream from: {review_silver}")
print(f"Schema validation: Strict (mergeSchema=false)")

# COMMAND ----------

# DBTITLE 1,Transform to Fact with Dimension Lookups
# Transform review stream to fact structure with dimension key lookups
fact_review_stream = (review_stream
    # Join with Business dimension (stream-static join)
    .join(
        broadcast(dim_business_df),
        review_stream.business_id == dim_business_df.NK_business_id,
        "left"  # Left join to capture missing dimension keys
    )
    
    # Join with User dimension (stream-static join)
    .join(
        broadcast(dim_user_df),
        review_stream.user_id == dim_user_df.NK_user_id,
        "left"  # Left join to capture missing dimension keys
    )
    
    # Join with Date dimension (stream-static join)
    .join(
        broadcast(dim_date_df),
        review_stream.review_date == dim_date_df.date_actual,
        "left"  # Left join to capture missing dimension keys
    )
    
    # Select fact columns
    .select(
        # Natural Key (for deduplication)
        col("review_id").alias("NK_review_id"),
        
        # Foreign Keys (Surrogate Keys from dimensions)
        col("SK_business_id"),
        col("SK_user_id"),
        col("SK_date_id"),
        
        # Measures
        col("stars"),
        col("review_useful_votes"),
        col("review_funny_votes"),
        col("review_cool_votes"),
        
        # Degenerate dimensions (stored in fact)
        col("review_text"),
        
        # Data quality flags
        col("SK_business_id").isNotNull().alias("dq_business_key_valid"),
        col("SK_user_id").isNotNull().alias("dq_user_key_valid"),
        col("SK_date_id").isNotNull().alias("dq_date_key_valid"),
        
        # Lineage
        col("_bronze_processing_date"),
        col("_silver_processing_date")
    )
    
    # Add fact metadata
    .withColumn("fact_created_date", current_timestamp())
)

print("Fact transformation configured:")
print("  ✓ Stream-static joins with Business, User, Date dimensions")
print("  ✓ Broadcast hints applied for dimension lookups")
print("  ✓ Left joins to capture missing dimension keys")
print("  ✓ Data quality flags added for each foreign key")

# COMMAND ----------

# DBTITLE 1,Split Pass & Quarantine Logic
def process_fact_microbatch(microBatchDF, batchId):
    """
    Process fact micro-batch:
    1. Deduplicate on NK_review_id (keep latest)
    2. Split into PASS (all keys valid) and QUARANTINE (missing keys)
    3. Write PASS to fact table (append-only)
    4. Write QUARANTINE to common DQ issues table with full schema
    """
    
    # Deduplicate within micro-batch
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("NK_review_id").orderBy(col("_bronze_processing_date").desc())
    
    deduplicated_df = (microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    if deduplicated_df.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return
    
    # Split PASS vs QUARANTINE based on dimension key validity
    pass_df = deduplicated_df.filter(
        col("dq_business_key_valid") &
        col("dq_user_key_valid") &
        col("dq_date_key_valid")
    )
    
    quarantine_df = deduplicated_df.filter(
        ~col("dq_business_key_valid") |
        ~col("dq_user_key_valid") |
        ~col("dq_date_key_valid")
    )
    
    pass_count = pass_df.count()
    quarantine_count = quarantine_df.count()
    
    print(f"Batch {batchId}: {pass_count} PASS, {quarantine_count} QUARANTINE")
    
    # Write PASS records to fact table (append-only)
    if pass_count > 0:
        fact_columns = [
            "NK_review_id",
            "SK_business_id",
            "SK_user_id",
            "SK_date_id",
            "stars",
            "review_useful_votes",
            "review_funny_votes",
            "review_cool_votes",
            "review_text",
            "_bronze_processing_date",
            "_silver_processing_date",
            "fact_created_date"
        ]
        
        (pass_df.select(fact_columns)
            .write
            .format("delta")
            .mode("append")  # Fact table is append-only
            .saveAsTable(fact_review)
        )
        print(f"  ✓ Wrote {pass_count} records to {fact_review}")
    
    # Write QUARANTINE records to common DQ issues table
    if quarantine_count > 0:
        # Build failed columns array based on which dimension keys are missing
        failed_cols_expr = F.array_distinct(
            F.filter(
                F.array(
                    F.when(~col("dq_business_key_valid"), F.lit("SK_business_id")).otherwise(F.lit(None)),
                    F.when(~col("dq_user_key_valid"), F.lit("SK_user_id")).otherwise(F.lit(None)),
                    F.when(~col("dq_date_key_valid"), F.lit("SK_date_id")).otherwise(F.lit(None))
                ),
                lambda x: x.isNotNull()
            )
        )
        
        # Build detailed reason
        dq_reason_expr = F.concat_ws(", ",
            F.when(~col("dq_business_key_valid"), F.lit("Missing business dimension key")).otherwise(F.lit(None)),
            F.when(~col("dq_user_key_valid"), F.lit("Missing user dimension key")).otherwise(F.lit(None)),
            F.when(~col("dq_date_key_valid"), F.lit("Missing date dimension key")).otherwise(F.lit(None))
        )
        
        # Transform to common DQ issues schema
        dq_issues = (quarantine_df
            .withColumn("dataset", F.lit("review"))
            .withColumn("table_name", F.lit(review_silver))
            .withColumn("layer", F.lit("gold"))
            .withColumn("record_key", F.create_map(
                F.lit("NK_review_id"), col("NK_review_id")
            ))
            .withColumn("dq_category", F.lit("MISSING_KEY"))
            .withColumn("dq_reason", dq_reason_expr)
            .withColumn("failed_columns", failed_cols_expr)
            .withColumn("original_record", F.to_json(F.struct("*")))
            .withColumn("pipeline_run_id", F.lit(_pipeline_run_id))
            .withColumn("quarantine_timestamp", current_timestamp())
            .withColumn("is_resolved", F.lit(False))
            .withColumn("resolved_timestamp", F.lit(None).cast("timestamp"))
            .withColumn("resolution_notes", F.lit(None).cast("string"))
            .select(
                "dataset",
                "table_name",
                "layer",
                "record_key",
                "dq_category",
                "dq_reason",
                "failed_columns",
                "original_record",
                "pipeline_run_id",
                "quarantine_timestamp",
                "is_resolved",
                "resolved_timestamp",
                "resolution_notes"
            )
        )
        
        (dq_issues
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(dq_table)
        )
        print(f"  • Quarantined {quarantine_count} records to {dq_table}")

print("Fact processing function defined")
print("  ✓ Deduplication by NK_review_id")
print("  ✓ PASS: All dimension keys valid → fact table (append)")
print("  ✓ QUARANTINE: Missing dimension keys → common DQ issues table")
print("  ✓ DQ schema includes: dataset, layer, record_key, failed_columns, original_record")

# COMMAND ----------

# DBTITLE 1,Start Fact Streaming Query
# Start Fact Review Stream
query_fact = (fact_review_stream
    .writeStream
    .foreachBatch(process_fact_microbatch)
    .option("checkpointLocation", checkpoint_path)
    .queryName("stream-gold_fact_review")
    .trigger(availableNow=True)  # Process all available data, then stop
    .start()
)


# COMMAND ----------

# DBTITLE 1,Wait for Stream  - Review (Demo only)
# This step is for demo only. 
#  - In Databricks Jobs, you do not need to use awaitTermination() because the job automatically waits for all streaming queries to finish before exiting.

import time

# Stream info
print(f"Streaming query started: {query_fact.name}")
print(f"Query ID: {query_fact.id}")
print(f"Checkpoint: {checkpoint_path}")
print(f"Target:")
print(f"  • Clean records → {fact_review}")
print(f"  • DQ failures → {dq_table}")

print("\nWaiting for stream to process all available data...")
query_fact.awaitTermination()

print("\n✓ Stream completed successfully")

# COMMAND ----------

# DBTITLE 1,Verify Fact Table
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_reviews,
# MAGIC   COUNT(DISTINCT NK_review_id) as unique_reviews,
# MAGIC   COUNT(DISTINCT SK_business_id) as unique_businesses,
# MAGIC   COUNT(DISTINCT SK_user_id) as unique_users,
# MAGIC   COUNT(DISTINCT SK_date_id) as unique_dates,
# MAGIC   ROUND(AVG(stars), 2) as avg_rating,
# MAGIC   SUM(review_useful_votes) as total_useful_votes
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_gold_fact_review

# COMMAND ----------

# DBTITLE 1,Check Quarantine Records
# MAGIC %sql
# MAGIC -- Check data quality issues from the common DQ issues table
# MAGIC SELECT
# MAGIC   COUNT(*) as total_issues,
# MAGIC   dq_category,
# MAGIC   dq_reason,
# MAGIC   failed_columns,
# MAGIC   COUNT(*) as count_by_issue
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_dq_common_issues
# MAGIC WHERE dataset = 'review' AND layer = 'gold'
# MAGIC GROUP BY dq_category, dq_reason, failed_columns
# MAGIC ORDER BY count_by_issue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_dq_common_issues
# MAGIC WHERE dataset = 'review' AND layer = 'gold'
# MAGIC GROUP BY dq_category, dq_reason, failed_columns
# MAGIC ORDER BY count_by_issue DESC