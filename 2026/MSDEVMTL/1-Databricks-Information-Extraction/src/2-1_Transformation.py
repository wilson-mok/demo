# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation: 
# MAGIC ## Step 1: Create Silver table using ai_parse_document() function
# MAGIC - Key benefits: https://www.databricks.com/blog/pdfs-production-announcing-state-art-document-intelligence-databricks
# MAGIC - Reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/ai_parse_document
# MAGIC - Note: This is a preview feature. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,Setup
bronze_table = "bronze_receipts"
silver_table = "silver_receipts"

# Identify image files (e.g., by extension)
image_extensions = ['.jpg', '.jpeg', '.png', '.bmp', '.gif']

# COMMAND ----------

from pyspark.sql.functions import *

# Read from bronze_receipt
bronze_df = spark.table(f"demo_catalog.demo_receipts.{bronze_table}")

display(bronze_df.limit(10))

# COMMAND ----------

# Build filter expression for image files
is_image_expr = " OR ".join([f"lower(src_file_name) LIKE '%{ext}'" for ext in image_extensions])

# Filter only image files
image_df = bronze_df.filter(expr(is_image_expr))

# Apply ai_parse_document to image files and extract metadata version
silver_df = image_df \
    .withColumn(
        "ai_parse_output",
        expr("ai_parse_document(raw_parsed)")
    ) \
    .withColumn(
        "ai_parse_error",
        expr("ai_parse_output:error_status")
    ) \
    .withColumn(
        "ai_parse_text",
        when(
            expr("try_variant_get(ai_parse_error, '$', 'string')").isNull(),
            expr("""
                concat_ws(
                    '\\n\\n',
                    transform(
                        try_cast(ai_parse_output:document:elements AS ARRAY<VARIANT>),
                        element -> try_cast(element:content AS STRING)
                    )
                ) AS text
            """)
        ).otherwise(lit(None))
    ) \
    .withColumnRenamed(
        "metadata_processing_timestamp", "metadata_src_processing_timestamp"
    ) \
    .withColumn(
        "metadata_processing_timestamp",
        current_timestamp()
    ) \
    .withColumn(
        "metadata_ai_parse_version",
        when(
            expr("try_variant_get(ai_parse_error, '$', 'string')").isNull(),
            expr("ai_parse_output:metadata.version")
        ).otherwise(lit(None))
    ) \
    .select(
        "ai_parse_output",
        "ai_parse_text",
        "ai_parse_error",
        "metadata_src_file_path",
        "metadata_src_processing_timestamp",
        "metadata_processing_timestamp",
        "metadata_ai_parse_version"
    )

silver_df.write.mode("overwrite").saveAsTable(f"demo_catalog.demo_receipts.{silver_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM demo_catalog.demo_receipts.silver_receipts;