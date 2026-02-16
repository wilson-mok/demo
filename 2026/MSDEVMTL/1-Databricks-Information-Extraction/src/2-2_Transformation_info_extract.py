# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation: 
# MAGIC
# MAGIC ## Step 2: Create your Information Extraction agent and endpoint
# MAGIC * To create the Intelligence Document agent, I would copy a sample set of silver_receipts data into a training table. 
# MAGIC * This training dataset will be the 'Ground truth'.
# MAGIC * After the Information Extraction agent has been created, update the endpoint parameter. 
# MAGIC * References: https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-bricks/key-info-extraction
# MAGIC
# MAGIC ## Step 3: Use the Information Extraction endpoint to extract key context in the Silver table
# MAGIC * use ai_query function to call the extraction endpoint
# MAGIC * The endpoint returns JSON as an output. Need to transform it into columns with schema evolution enabled.
# MAGIC * Note: In a day-to-day processing, I would combined both transformation steps together. 
# MAGIC * Reference: 
# MAGIC   * ai_query: https://learn.microsoft.com/en-us/azure/databricks/large-language-models/ai-functions#gen-function
# MAGIC   * Schema evolution: https://learn.microsoft.com/en-us/azure/databricks/data-engineering/schema-evolution

# COMMAND ----------

silver_table = "silver_receipts"
silver_table_info_extract = "silver_receipts_info_extract"

# Parameter
receipt_extraction_endpoint = dbutils.widgets.get("receipt_extraction_endpoint")

# COMMAND ----------

from pyspark.sql.functions import *

# Read from silver_receipts
silver_df = spark.table(f"demo_catalog.demo_receipts.{silver_table}")

display(silver_df.limit(10))

# COMMAND ----------

silver_info_extract_df = silver_df.selectExpr(
    "*",
    f"ai_query('{receipt_extraction_endpoint}', ai_parse_text) as ai_extract_result"
)

display(silver_info_extract_df.limit(10))

# COMMAND ----------

test_df = silver_info_extract_df.selectExpr(
    "ai_extract_result:address::string as address",
    "ai_extract_result:merchant_name::string as merchant_name",
    "ai_extract_result:receipt_datetime::string as receipt_datetime",
    "ai_extract_result:receipt_number::string as receipt_number",
    "ai_extract_result:sub_total_after_discount::decimal(10,2) as sub_total_after_discount",
    "ai_extract_result:tax::decimal(10,2) as tax",
    "ai_extract_result:total::decimal(10,2) as total",
    "*"
).drop("ai_extract_result")

display(test_df)

# COMMAND ----------

# DBTITLE 1,Cell 5
from pyspark.sql.types import StringType, DecimalType, TimestampType
from pyspark.sql.functions import col

# Extract fields from ai_extract_result variant into individual columns with correct types
silver_info_extract_flat_df = silver_info_extract_df.selectExpr(
    "ai_extract_result:address::string as address",
    "ai_extract_result:merchant_name::string as merchant_name",
    "ai_extract_result:receipt_datetime::string as receipt_datetime",
    "ai_extract_result:receipt_number::string as receipt_number",
    "ai_extract_result:sub_total_after_discount::decimal(10,2) as sub_total_after_discount",
    "ai_extract_result:tax::decimal(10,2) as tax",
    "ai_extract_result:total::decimal(10,2) as total",
    "*"
)

# Write to table
#   For a production pipeline, the mode = append instead of overwrite
(
    silver_info_extract_flat_df
    .write
    .mode("overwrite")
    .option("mergeSchema", "true").saveAsTable(f"demo_catalog.demo_receipts.{silver_table_info_extract}")
)

# Display result
display(silver_info_extract_flat_df)