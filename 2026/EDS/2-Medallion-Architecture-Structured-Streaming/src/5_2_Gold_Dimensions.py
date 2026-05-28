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

# Source tables (Silver)
business_silver = f"{catalog}.{schema}.{silver_prefix}business"
user_silver = f"{catalog}.{schema}.{silver_prefix}user"

# Target tables (Gold Dimensions)
dim_business = f"{catalog}.{schema}.{gold_prefix}dim_business"
dim_user = f"{catalog}.{schema}.{gold_prefix}dim_user"

# Checkpoint locations
checkpoint_business = f"/Volumes/{catalog}/{schema}/checkpoints/{gold_prefix}dim_business/"
checkpoint_user = f"/Volumes/{catalog}/{schema}/checkpoints/{gold_prefix}dim_user/"

print(f"Pipeline Run ID: {_pipeline_run_id}")
print(f"\nSource Tables:")
print(f"  Business Silver: {business_silver}")
print(f"  User Silver: {user_silver}")
print(f"\nTarget Dimensions:")
print(f"  dim_business: {dim_business}")
print(f"  dim_user: {dim_user}")
print(f"\nCheckpoints:")
print(f"  Business: {checkpoint_business}")
print(f"  User: {checkpoint_user}")

# COMMAND ----------

# DBTITLE 1,Reset Data (Demo only)
# ** For Demo or Development/Testing only **
#   Drop tables and checkpoints if reset flag is set

print(f"Reset Before Run: {reset_before_run}")
if reset_before_run == "true":
    print("\n=== RESETTING: Dropping tables and checkpoints ===")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {dim_business}")
        dbutils.fs.rm(checkpoint_business, recurse=True)
        print(f"  ✓ Reset {dim_business}")
    except Exception as e:
        print(f"  • Error: {str(e)}")
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {dim_user}")
        dbutils.fs.rm(checkpoint_user, recurse=True)
        print(f"  ✓ Reset {dim_user}")
    except Exception as e:
        print(f"  • Error: {str(e)}")

    print("=== Reset complete ===\n")
else:
    print("Skipping reset")

# COMMAND ----------

# DBTITLE 1,Business Dimension Transformation
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

# Read streaming from Silver Business
business_stream = (spark.readStream
    .format("delta")
    .option("mergeSchema", "false")
    .table(business_silver)
)

# Transform to dimension structure with surrogate keys
dim_business_stream = (business_stream
    # Rename business_id to natural key first
    .withColumnRenamed("business_id", "NK_business_id")
    
    # Generate Surrogate Key: Deterministic UUID from natural key (MD5 hash)
    .withColumn("SK_business_id", F.md5(col("NK_business_id")))
    
    # Select dimension attributes (SCD Type 1 - all attributes)
    .select(
        "SK_business_id",
        "NK_business_id",
        "name",
        "address",
        "city",
        "state",
        "postal_code",
        "latitude",
        "longitude",
        "lifetime_avg_stars",
        "lifetime_review_count",
        "is_open",
        "attributes",
        "categories",
        "hours"
    )
    
    # Add dimension metadata
    .withColumn("dim_created_date", current_timestamp())
    .withColumn("dim_updated_date", current_timestamp())
)

print(f"Business dimension stream configured")
print(f"  Source: {business_silver}")
print(f"  Target: {dim_business}")
print(f"  SCD Type: 1 (Overwrite on change)")
print(f"  Surrogate Key: Deterministic UUID (MD5 of NK)")

# COMMAND ----------

# DBTITLE 1,Business Dimension Upsert Logic
def upsert_business_dim(microBatchDF, batchId):
    """
    SCD Type 1 Upsert for Business Dimension:
    - INSERT new businesses (by NK_business_id)
    - UPDATE existing businesses (overwrite all attributes)
    """
    
    # Deduplicate within micro-batch - keep latest by _bronze_processing_date
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("NK_business_id").orderBy(col("dim_updated_date").desc())
    
    deduplicated_df = (microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    if deduplicated_df.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return
    
    # Check if dimension table exists
    if spark.catalog.tableExists(dim_business):
        # MERGE for SCD Type 1
        dim_table = DeltaTable.forName(spark, dim_business)
        
        (dim_table.alias("target")
            .merge(
                deduplicated_df.alias("source"),
                "target.NK_business_id = source.NK_business_id"
            )
            .whenMatchedUpdateAll()  # SCD Type 1: Overwrite all columns
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Batch {batchId}: Merged into {dim_business}")
    else:
        # First run: Create table
        (deduplicated_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(dim_business)
        )
        print(f"Batch {batchId}: Created {dim_business}")

print("Business dimension upsert function defined")

# COMMAND ----------

# DBTITLE 1,User Dimension Transformation
# Read streaming from Silver User
user_stream = (spark.readStream
    .format("delta")
    .option("mergeSchema", "false")
    .table(user_silver)
)

# Transform to dimension structure with surrogate keys
dim_user_stream = (user_stream
    # Rename user_id to natural key first
    .withColumnRenamed("user_id", "NK_user_id")
    
    # Generate Surrogate Key: Deterministic UUID from natural key (MD5 hash)
    .withColumn("SK_user_id", F.md5(col("NK_user_id")))
    
    # Calculate friends_count from friends_array
    .withColumn("friends_count", F.size(col("friends_array")))
    
    # Select dimension attributes (SCD Type 1 - all attributes)
    .select(
        "SK_user_id",
        "NK_user_id",
        "name",
        "member_since",
        "lifetime_review_count",
        "useful",
        "funny",
        "cool",
        "elite_years",
        "friends_count",
        "fans",
        "lifetime_avg_stars",
        "compliment_hot",
        "compliment_more",
        "compliment_profile",
        "compliment_cute",
        "compliment_list",
        "compliment_note",
        "compliment_plain",
        "compliment_cool",
        "compliment_funny",
        "compliment_writer",
        "compliment_photos"
    )
    
    # Add dimension metadata
    .withColumn("dim_created_date", current_timestamp())
    .withColumn("dim_updated_date", current_timestamp())
)

print(f"User dimension stream configured")
print(f"  Source: {user_silver}")
print(f"  Target: {dim_user}")
print(f"  SCD Type: 1 (Overwrite on change)")
print(f"  Surrogate Key: Deterministic UUID (MD5 of NK)")

# COMMAND ----------

# DBTITLE 1,User Dimension Upsert Logic
def upsert_user_dim(microBatchDF, batchId):
    """
    SCD Type 1 Upsert for User Dimension:
    - INSERT new users (by NK_user_id)
    - UPDATE existing users (overwrite all attributes)
    """
    
    # Deduplicate within micro-batch - keep latest by _bronze_processing_date
    from pyspark.sql.window import Window
    window_spec = Window.partitionBy("NK_user_id").orderBy(col("dim_updated_date").desc())
    
    deduplicated_df = (microBatchDF
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )
    
    if deduplicated_df.count() == 0:
        print(f"Batch {batchId}: No records to process")
        return
    
    # Check if dimension table exists
    if spark.catalog.tableExists(dim_user):
        # MERGE for SCD Type 1
        dim_table = DeltaTable.forName(spark, dim_user)
        
        (dim_table.alias("target")
            .merge(
                deduplicated_df.alias("source"),
                "target.NK_user_id = source.NK_user_id"
            )
            .whenMatchedUpdateAll()  # SCD Type 1: Overwrite all columns
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"Batch {batchId}: Merged into {dim_user}")
    else:
        # First run: Create table
        (deduplicated_df
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(dim_user)
        )
        print(f"Batch {batchId}: Created {dim_user}")

print("User dimension upsert function defined")

# COMMAND ----------

# DBTITLE 1,Start Streaming - Business
# Start Business Dimension Stream
query_business = (dim_business_stream
    .writeStream
    .foreachBatch(upsert_business_dim)
    .option("checkpointLocation", checkpoint_business)
    .queryName("stream-gold_dim_business")
    .trigger(availableNow=True)  # Process all available data, then stop
    .start()
)


# COMMAND ----------

# DBTITLE 1,Wait for Stream  - Business (Demo only)
# This step is for demo only. 
#  - In Databricks Jobs, you do not need to use awaitTermination() because the job automatically waits for all streaming queries to finish before exiting.

import time

# Stream info
print(f"Streaming query started: {query_business.name}")
print(f"Query ID: {query_business.id}")
print(f"Checkpoint: {checkpoint_business}")
print(f"Target: {dim_business}")

print("\nWaiting for stream to process all available data...")

query_business.awaitTermination()

print("\n✓ Stream completed successfully")

# COMMAND ----------

# DBTITLE 1,Start Streaming - User
# Start User Dimension Stream
query_user = (dim_user_stream
    .writeStream
    .foreachBatch(upsert_user_dim)
    .option("checkpointLocation", checkpoint_user)
    .queryName("stream-gold_dim_user")
    .trigger(availableNow=True)  # Process all available data, then stop
    .start()
)

# COMMAND ----------

# DBTITLE 1,Wait for Stream  - User (Demo only)
# This step is for demo only. 
#  - In Databricks Jobs, you do not need to use awaitTermination() because the job automatically waits for all streaming queries to finish before exiting.

import time

# Stream info
print(f"Streaming query started: {query_user.name}")
print(f"Query ID: {query_user.id}")
print(f"Checkpoint: {checkpoint_user}")
print(f"Target: {dim_user}")

print("\nWaiting for stream to process all available data...")

query_user.awaitTermination()

print("\n✓ Stream completed successfully")

# COMMAND ----------

# DBTITLE 1,Verify Business Dimension
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_businesses,
# MAGIC   COUNT(DISTINCT SK_business_id) as unique_sk,
# MAGIC   COUNT(DISTINCT NK_business_id) as unique_nk,
# MAGIC   AVG(lifetime_avg_stars) as avg_stars,
# MAGIC   SUM(CASE WHEN is_open = 1 THEN 1 ELSE 0 END) as open_businesses,
# MAGIC   SUM(CASE WHEN is_open = 0 THEN 1 ELSE 0 END) as closed_businesses
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_gold_dim_business

# COMMAND ----------

# DBTITLE 1,Verify User Dimension
# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*) as total_users,
# MAGIC   COUNT(DISTINCT SK_user_id) as unique_sk,
# MAGIC   COUNT(DISTINCT NK_user_id) as unique_nk,
# MAGIC   AVG(lifetime_review_count) as avg_reviews,
# MAGIC   MIN(member_since) as earliest_member,
# MAGIC   MAX(member_since) as latest_member
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_gold_dim_user