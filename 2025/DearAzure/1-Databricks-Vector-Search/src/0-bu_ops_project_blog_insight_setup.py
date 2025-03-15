# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Clean up
# MAGIC -- DROP SCHEMA IF EXISTS bu_ops.project_blog_insight CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE CATALOG IF NOT EXISTS bu_ops;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC USE CATALOG bu_ops;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS project_blog_insight;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE bu_ops.project_blog_insight;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS blog_volume;

# COMMAND ----------

# Force install package, if needed.

%pip install --upgrade --force-reinstall databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# Create the Vector Search Endpoint
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vsc.create_endpoint(
  name="vs_blog_insight_schiiss_endpoint",
  endpoint_type="STANDARD"
)

# This endpoint will take couple min to create. While waiting, run 1-blog_ingest_and_processing notebook. 