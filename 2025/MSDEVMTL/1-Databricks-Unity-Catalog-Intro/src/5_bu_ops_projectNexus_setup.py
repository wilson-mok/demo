# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Clean up
# MAGIC -- DROP SCHEMA IF EXISTS bu_ops.projectNexus CASCADE;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC USE CATALOG bu_ops;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS projectNexus;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Create View
# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG bu_ops;
# MAGIC USE SCHEMA projectNexus;
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS dev_Consumption_ro AS
# MAGIC   SELECT dimSc.zipCode, dimDt.calendarDate, avg(factPc.powerConsumptedKwh) as avgConsumption
# MAGIC   FROM ent_dev.gold_operation.factpowerconsumption factPc
# MAGIC   JOIN ent_dev.gold_operation.dimpowermeter dimPm ON (factPc.meterSid = dimPm.sid) 
# MAGIC   JOIN ent_dev.gold_common.dimdate dimDt ON (factPc.measurementDateSid = dimDt.sid)
# MAGIC   JOIN ent_dev.gold_common.dimservicelocation dimSc ON (factPc.serviceLocationSid = dimSc.sid)
# MAGIC   GROUP BY dimSc.zipCode, dimDt.calendarDate

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM dev_Consumption_ro;

# COMMAND ----------

# DBTITLE 1,ent_dev is read-only
# MAGIC %sql
# MAGIC
# MAGIC -- We should be getting an error for Read-only. 
# MAGIC
# MAGIC UPDATE ent_dev.gold_common.dimservicelocation
# MAGIC SET _pipeline_run_id = 0
# MAGIC WHERE _pipeline_run_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- As the power costs are going up, home owners and businesses are going to be adopt more solar power. Based on our market analysis, we expected specific zip codes to have higher adoption rates (CSV file).
# MAGIC
# MAGIC -- Update the file to create a Managed Table.
# MAGIC
# MAGIC SELECT curConsumption.zipCode as zip_code, 
# MAGIC        curConsumption.avgConsumption as cur_avg_consumption_per_day,
# MAGIC        CASE 
# MAGIC         WHEN prjConsumption.impact IS NOT NULL THEN curConsumption.avgConsumption * (1+prjConsumption.impact)
# MAGIC         ELSE curConsumption.avgConsumption * 1.1
# MAGIC       END as proj_avg_consumption_per_day
# MAGIC FROM projectNexus.dev_Consumption_ro curConsumption
# MAGIC LEFT JOIN projectNexus.energy_consumption_projection prjConsumption ON (curConsumption.zipCode = prjConsumption.zipCode)
# MAGIC
# MAGIC -- 98087

# COMMAND ----------

# DBTITLE 1,Create and upload data to a Managed Volume
# MAGIC %sql
# MAGIC
# MAGIC CREATE VOLUME bu_ops.projectnexus.project_files;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Upload the parquet file. 
# MAGIC
# MAGIC CREATE VIEW IF NOT EXISTS bu_ops.projectnexus.test_view AS 
# MAGIC   SELECT *
# MAGIC   FROM PARQUET.`/Volumes/bu_ops/projectnexus/project_files/energy_consumption_projection.snappy.parquet`;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM bu_ops.projectnexus.test_view;