# Databricks notebook source
# create parameters

dbutils.widgets.text("env", "ent_dev")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM system.access.audit
# MAGIC WHERE user_identity.email = 'demo@wdotcode.com'
# MAGIC order by event_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Enable Audit Logs: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/#enable-a-system-schema
# MAGIC -- Audit Logs - Sample Queries: https://learn.microsoft.com/en-us/azure/databricks/admin/system-tables/audit-logs#table-access
# MAGIC
# MAGIC SELECT user_identity.email as identity_email, request_params['full_name_arg'] as table_name, workspace_id, action_name, event_time, event_id
# MAGIC FROM system.access.audit
# MAGIC WHERE action_name IN ('createTable', 'getTable', 'deleteTable')
# MAGIC   and request_params['full_name_arg'] = '${env}.gold_operation.factpowerconsumption'
# MAGIC   and user_identity.email = 'demo@wdotcode.com'
# MAGIC order by event_time DESC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create a tag for the fact table. You dont need admin permission to create this. 
# MAGIC
# MAGIC ALTER TABLE ${env}.gold_operation.factPowerConsumption
# MAGIC SET TAGS ('test'='')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM system.information_schema.table_tags

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Query the Audit logs using tags
# MAGIC SELECT user_identity.email as identity_email, request_params['full_name_arg'] as table_name, workspace_id, action_name, event_time, event_id
# MAGIC FROM system.access.audit audit
# MAGIC JOIN system.information_schema.table_tags tableTags 
# MAGIC   ON (audit.request_params['full_name_arg'] = CONCAT(tableTags.catalog_name, '.', tableTags.schema_name, '.', tableTags.table_name))
# MAGIC WHERE tableTags.tag_name = 'test'
# MAGIC order by audit.event_time DESC;