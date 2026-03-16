# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion (batch)
# MAGIC * The receipts are images, so we are reading in as binary.
# MAGIC * The raw_parsed column (type: binary) will be used by ai_parse_document to extract structured content from the image file (unstructured data).
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup
bronze_table = "bronze_receipts"

# Parameters
src_volume_path = dbutils.widgets.get("srcVolumePath")

# COMMAND ----------

# DBTITLE 0,Cell 3
from pyspark.sql import functions as F

df = (spark.read
      .format("binaryFile")
      .load(src_volume_path))

bronze_df = df.withColumnRenamed("content", "raw_parsed") \
    .withColumnRenamed("path", "metadata_src_file_path") \
    .withColumnRenamed("length", "metadata_src_file_size_bytes") \
    .withColumnRenamed("modificationTime", "metadata_src_file_modified_at") \
    .withColumn("src_file_name", F.element_at(F.split(F.col("metadata_src_file_path"), "/"), -1)) \
    .withColumn("metadata_processing_timestamp", F.current_timestamp())

bronze_df.write.mode("overwrite").saveAsTable(f"demo_catalog.demo_receipts.{bronze_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM demo_catalog.demo_receipts.bronze_receipts;