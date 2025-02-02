# Databricks notebook source
# DBTITLE 1,Clean up
# MAGIC %sql 
# MAGIC
# MAGIC -- ent_dev
# MAGIC -- DROP CATALOG IF EXISTS ent_dev CASCADE;
# MAGIC -- DROP EXTERNAL LOCATION IF EXISTS ext_loc_ent_dev_landing;
# MAGIC -- DROP EXTERNAL LOCATION IF EXISTS ext_loc_ent_dev_bronze;
# MAGIC -- DROP EXTERNAL LOCATION IF EXISTS ext_loc_ent_dev_silver;
# MAGIC -- DROP EXTERNAL LOCATION IF EXISTS ext_loc_ent_dev_gold;
# MAGIC

# COMMAND ----------

# DBTITLE 1,ent_dev - EXTERNAL LOCATION setup
# MAGIC %sql
# MAGIC -- 1. Create the Storage Credentials first using the Catalog Explorer or Databricks CLI
# MAGIC --   https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/storage-credentials
# MAGIC
# MAGIC -- 2. Create the External Location and apply permissions and isolated workspace
# MAGIC --   https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/
# MAGIC --   Privilege Type: https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges#privilege-types
# MAGIC
# MAGIC -- External Location: Landing
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ent_dev_landing 
# MAGIC   URL "abfss://landing@staadbdemoentdatadev.dfs.core.windows.net/"
# MAGIC   WITH (STORAGE CREDENTIAL sta_cred_ent_dev)
# MAGIC   COMMENT 'External location for the ENT Dev Landing/Raw data';
# MAGIC
# MAGIC GRANT BROWSE ON EXTERNAL LOCATION ext_loc_ent_dev_landing TO `account users`;
# MAGIC GRANT CREATE EXTERNAL VOLUME ON EXTERNAL LOCATION ext_loc_ent_dev_landing TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION ext_loc_ent_dev_landing TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC
# MAGIC -- External Location: Bronze
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ent_dev_bronze 
# MAGIC   URL "abfss://bronze@staadbdemoentdatadev.dfs.core.windows.net/" 
# MAGIC   WITH (STORAGE CREDENTIAL sta_cred_ent_dev)
# MAGIC   COMMENT 'External location for the ENT Dev Bronze data';
# MAGIC
# MAGIC GRANT BROWSE ON EXTERNAL LOCATION ext_loc_ent_dev_bronze TO `account users`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_loc_ent_dev_bronze TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION ext_loc_ent_dev_bronze TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION ext_loc_ent_dev_bronze TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC
# MAGIC -- External Location: Silver
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ent_dev_silver 
# MAGIC URL "abfss://silver@staadbdemoentdatadev.dfs.core.windows.net/" 
# MAGIC WITH (STORAGE CREDENTIAL sta_cred_ent_dev)
# MAGIC COMMENT 'External location for the ENT Dev Silver data';
# MAGIC
# MAGIC GRANT BROWSE ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `account users`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC
# MAGIC -- External Location: Gold
# MAGIC CREATE EXTERNAL LOCATION IF NOT EXISTS ext_loc_ent_dev_gold 
# MAGIC URL "abfss://gold@staadbdemoentdatadev.dfs.core.windows.net/" 
# MAGIC WITH (STORAGE CREDENTIAL sta_cred_ent_dev)
# MAGIC COMMENT 'External location for the ENT Dev Gold data';
# MAGIC
# MAGIC GRANT BROWSE ON EXTERNAL LOCATION ext_loc_ent_dev_gold TO `account users`;
# MAGIC GRANT CREATE EXTERNAL TABLE ON EXTERNAL LOCATION ext_loc_ent_dev_gold TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT READ FILES ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT WRITE FILES ON EXTERNAL LOCATION ext_loc_ent_dev_silver TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Restrict access to External Locations
# MAGIC %sql
# MAGIC -- 3. For Landing external location, use the Catalog Explorer to enable 'Limit to read-only use'
# MAGIC --  https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/manage-external-locations#mark-an-external-location-as-read-only
# MAGIC
# MAGIC -- 4. Bind the external locations above to the admin and dev workspace only. (Preivew)
# MAGIC --  https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations#bind
# MAGIC --  As of Jan 26, 2025, there is a bug which requires you to use the 'Workspace Bindings' API. 

# COMMAND ----------

# DBTITLE 1,ent_dev - CATALOG & SCHEMA setup
# MAGIC %sql
# MAGIC
# MAGIC -- 5. Create Catalog, Schemas and Landing Volume
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ent_dev COMMENT 'Enterprise Data - DEV Catalog';
# MAGIC GRANT ALL PRIVILEGES ON CATALOG ent_dev TO `Demo_Ent-Data-Group_Non-Prod`;
# MAGIC GRANT USE CATALOG ON CATALOG ent_dev TO `Demo_Ops-Group`;
# MAGIC
# MAGIC USE CATALOG ent_dev;
# MAGIC
# MAGIC -- Schema: landing
# MAGIC CREATE SCHEMA IF NOT EXISTS landing;
# MAGIC
# MAGIC -- CREATE landing volume
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS landing.raw LOCATION 'abfss://landing@staadbdemoentdatadev.dfs.core.windows.net/';
# MAGIC
# MAGIC -- Schema for Bronze
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_corpSharedDrive;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_iotSmartGrid;
# MAGIC
# MAGIC -- Schema for Silver
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_common;
# MAGIC GRANT USE SCHEMA ON SCHEMA silver_common TO `Demo_Ops-Group`;
# MAGIC GRANT SELECT ON SCHEMA silver_common TO `Demo_Ops-Group`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_operation;
# MAGIC GRANT USE SCHEMA ON SCHEMA silver_operation TO `Demo_Ops-Group`;
# MAGIC GRANT SELECT ON SCHEMA silver_operation TO `Demo_Ops-Group`;
# MAGIC
# MAGIC
# MAGIC -- Grant Select permission to Operations
# MAGIC
# MAGIC -- Schema for Gold
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_common;
# MAGIC GRANT USE SCHEMA ON SCHEMA gold_common TO `Demo_Ops-Group`;
# MAGIC GRANT SELECT ON SCHEMA gold_common TO `Demo_Ops-Group`;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_operation;
# MAGIC GRANT USE SCHEMA ON SCHEMA gold_operation TO `Demo_Ops-Group`;
# MAGIC GRANT SELECT ON SCHEMA gold_operation TO `Demo_Ops-Group`;
# MAGIC
# MAGIC -- Grant Select permission to Operations
# MAGIC
# MAGIC -- 6. Bind the Catalog to 
# MAGIC --    a. admin and dev workspace - Read and Write. 
# MAGIC --    b. operations workspace - Read only
# MAGIC -- https://learn.microsoft.com/en-us/azure/databricks/catalogs/binding#bind

# COMMAND ----------

# DBTITLE 1,Sample - Check permissions
# MAGIC %sql
# MAGIC
# MAGIC SHOW GRANTS `Demo_Ent-Data-Group_Non-Prod` ON CATALOG ent_dev;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- You can use check the varies access using 'SHOW GRANTS'
# MAGIC
# MAGIC SHOW GRANTS ON EXTERNAL LOCATION ext_loc_ent_dev_landing;