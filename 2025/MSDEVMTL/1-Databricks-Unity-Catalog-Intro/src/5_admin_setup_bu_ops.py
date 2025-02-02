# Databricks notebook source
# MAGIC %sql 
# MAGIC
# MAGIC -- bu_ops
# MAGIC DROP CATALOG IF EXISTS bu_ops CASCADE;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 1. Create Catalog
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS bu_ops COMMENT 'BU - Operations Catalog';
# MAGIC GRANT ALL PRIVILEGES ON CATALOG bu_ops TO `Demo_Ops-Group`;
# MAGIC
# MAGIC -- 6. Bind the Catalog to the admin and operations workspace only. 
# MAGIC -- https://learn.microsoft.com/en-us/azure/databricks/catalogs/binding#bind

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW GRANTS `Demo_Ops-Group` ON CATALOG bu_ops;