# Databricks notebook source
# DBTITLE 1,Configuration and Imports
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# Parameters
dbutils.widgets.text("pipeline_run_id", "default_run", "Pipeline Run ID")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

dbutils.widgets.dropdown("reset_before_run", "false", ["true", "false"], "Reset Before Run")
reset_before_run = dbutils.widgets.get("reset_before_run")

# Configuration
base_path = "/Volumes/demo_catalog/demo_yelp_academic/raw_data"
target_catalog = "demo_catalog"
target_schema = "demo_yelp_academic"
target_table_prefix = "ss_bronze_"

# Dataset names
datasets = ["business", "review", "tip", "user"]

print(f"Pipeline Run ID: {pipeline_run_id}")

# COMMAND ----------

# DBTITLE 1,Reset data (Demo only)
# ** For Demo or Development/Testing only **
#   Auto cleanup if reset flag is set

print(f"Reset Before Run: {reset_before_run}")
if reset_before_run == "true":
    print("\n=== RESETTING: Dropping tables and checkpoints ===")
    for dataset in datasets:
        table_name = f"{target_table_prefix}{dataset}"
        full_table_name = f"{target_catalog}.{target_schema}.{table_name}"
        checkpoint_path = f"/Volumes/{target_catalog}/{target_schema}/checkpoints/{table_name}"
        
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            dbutils.fs.rm(checkpoint_path, recurse=True)
            print(f"  ✓ Reset {dataset}")
        except Exception as e:
            print(f"  • {dataset} - {str(e)}")
    print("=== Reset complete ===\n")

# COMMAND ----------

# DBTITLE 1,Auto Loader Function
def load_bronze_dataset(dataset_name, source_path, catalog, schema, table_prefix, pipeline_run_id):
    """
    Load data using Auto Loader with metadata columns
    """
    table_name = f"{table_prefix}{dataset_name}"
    checkpoint_path = f"/Volumes/{catalog}/{schema}/checkpoints/{table_name}"
    
    print(f"Processing {dataset_name}...")
    print(f"  Source: {source_path}")
    print(f"  Target: {catalog}.{schema}.{table_name}")
    
    # Read with Auto Loader
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint_path + "/_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(source_path)
    )
    
    # Add metadata columns
    df_with_metadata = (df
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("_processing_date", F.current_timestamp())
        .withColumn("_input_filename", F.col("_metadata.file_path"))
        .withColumn("_input_file_modification_date", 
                   F.col("_metadata.file_modification_time"))
    )
    
    # Write to Delta table
    query = (df_with_metadata.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{catalog}.{schema}.{table_name}")
    )
    
    # Wait for completion
    query.awaitTermination()
    print(f"  ✓ Completed {dataset_name}")
    
    return query

# COMMAND ----------

# DBTITLE 1,Process All Datasets
# Process each dataset
print(f"Processing datasets: {datasets}")

for dataset in datasets:
    source_path = f"{base_path}/{dataset}"
    
    try:
        load_bronze_dataset(
            dataset_name=dataset,
            source_path=source_path,
            catalog=target_catalog,
            schema=target_schema,
            table_prefix=target_table_prefix,
            pipeline_run_id=pipeline_run_id
        )
    except Exception as e:
        print(f"Error processing {dataset}: {str(e)}")
        raise

print("\n✓ All datasets processed successfully!")

# COMMAND ----------

# DBTITLE 1,Verify Loaded Tables
# MAGIC %sql
# MAGIC -- For demo purpose only
# MAGIC --  Check row counts for all bronze tables
# MAGIC
# MAGIC SELECT
# MAGIC   'ss_bronze_business' AS table_name,
# MAGIC   FORMAT_NUMBER(COUNT(*), 0) AS row_count,
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END) AS rescued_count
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_bronze_business
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'ss_bronze_review',
# MAGIC   FORMAT_NUMBER(COUNT(*), 0),
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_bronze_review
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'ss_bronze_tip',
# MAGIC   FORMAT_NUMBER(COUNT(*), 0),
# MAGIC   SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_bronze_tip
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC     'ss_bronze_user', 
# MAGIC     FORMAT_NUMBER(COUNT(*), 0),
# MAGIC     SUM(CASE WHEN _rescued_data IS NOT NULL THEN 1 ELSE 0 END)
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_bronze_user
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sample data
# MAGIC
# MAGIC SELECT *
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_bronze_business
# MAGIC LIMIT 10;