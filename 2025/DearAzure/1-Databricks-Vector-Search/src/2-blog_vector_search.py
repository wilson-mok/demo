# Databricks notebook source
# MAGIC %md
# MAGIC # References
# MAGIC - Vector Search
# MAGIC   - https://learn.microsoft.com/en-us/azure/databricks/generative-ai/create-query-vector-search
# MAGIC   - https://learn.microsoft.com/en-us/azure/databricks/generative-ai/tutorials/ai-cookbook/quality-data-pipeline-rag
# MAGIC   - https://learn.microsoft.com/en-us/azure/databricks/generative-ai/vector-search-best-practices
# MAGIC

# COMMAND ----------

# MAGIC %pip install --upgrade --force-reinstall databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

vsc_endpoint_name = "vs_blog_insight_schiiss_endpoint"

catalog = "bu_ops"
schema = "project_blog_insight"

embedding_model_endpoint = "databricks-bge-large-en"
src_table_name = f"{catalog}.{schema}.blog_schiiss"
vsc_index_name = f"{src_table_name}_index"


# COMMAND ----------

# DBTITLE 1,Get Vector Search endpoint
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vsc_endpoint = vsc.get_endpoint(vsc_endpoint_name)

vsc_endpoint

# COMMAND ----------

# DBTITLE 1,Create Vector index
# Index sync model: "Triggered" or "Continuous"
# https://learn.microsoft.com/en-us/azure/databricks/generative-ai/create-query-vector-search#create-index-using-the-ui

# Alternatively, you can create different type of index
# https://learn.microsoft.com/en-us/azure/databricks/generative-ai/vector-search#options-for-providing-vector-embeddings
# https://learn.microsoft.com/en-us/azure/databricks/generative-ai/create-query-vector-search#sync-embeddings-table

vsc_index = vsc.create_delta_sync_index(
    endpoint_name=vsc_endpoint_name,
    source_table_name=src_table_name,
    index_name=vsc_index_name,
    pipeline_type="TRIGGERED",
    primary_key="row_id",
    embedding_source_column="content",
    embedding_model_endpoint_name=embedding_model_endpoint,
    sync_computed_embeddings=True
)

# Catalog Explorer
#  bu_ops.project_blog_insight.blog_schiiss_index 
#   1. embedding column "__db_content_vector"
#   2. Other model we can use instead of "databricks-bge-large-en"
#  bu_ops.project_blog_insight.blog_schiiss_index_writeback_table
#   1.  This is the writeback table containing our data from the blog and our queries
#   -> Wait for status = Ready (5 min)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bu_ops.project_blog_insight.blog_schiiss_index_writeback_table
# MAGIC ORDER BY row_id desc;

# COMMAND ----------

# Lets do a search

results = vsc_index.similarity_search(
    query_text="Do we have any articles on LLM Memory?",
    columns=["row_id", "title", "content"],
    num_results=10
)

display(results["result"]["data_array"])

# COMMAND ----------

# Lets do a search with filter

results = vsc_index.similarity_search(
    query_text="Do we have any articles on RAG and how to chunk?",
    columns=["row_id", "title", "tags", "content"],
    num_results=10,
    filters={"tags NOT": "Cyber Security"}
)

display(results["result"]["data_array"])

# COMMAND ----------

# Clean up
vsc.delete_index(
    endpoint_name=vsc_endpoint_name,
    index_name=vsc_index_name
)

# COMMAND ----------

vsc.delete_endpoint(vsc_endpoint_name)