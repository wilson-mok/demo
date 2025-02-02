# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Create an external table for the [env].gold_common.dimServiceLocation with a pre-defined schema.
# MAGIC 1. Load the [env].silver_common.zipCodeGeoLocation based on the silver_processing_date parameter
# MAGIC 1. Merge the changes into the gold external table using the silver data.
# MAGIC

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1033")


# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "gold"

# Define the dataset details and location
businessDomain = "common"
datasetName = "dimServiceLocation"

# Src - Table
srcTableName = f"{env}.silver_common.zipCodeGeoLocation"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"


# COMMAND ----------

# Execute this to reset the demo

# spark.sql(f"DROP TABLE IF EXISTS {sqlFullName}")
# dbutils.fs.rm(goldLocation, True)

# COMMAND ----------

# create table, if required.

spark.sql(f"""
          CREATE EXTERNAL TABLE IF NOT EXISTS {targetTableName} (
              sid bigint GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
              zipCode int, -- Business Key
              longitude double,
              latitude double,
              _keyHash string,
              _valueHash string,
              _pipeline_run_id string,
              _processing_date timestamp)
          USING delta LOCATION '{targetLocation}' 
          """)

# COMMAND ----------

# retrieve the data that has been added today. 

from pyspark.sql.functions import *

dataSilver = spark.read.table(srcTableName)


# COMMAND ----------

# DBTITLE 1,Correct Data Structure issue and add metadata
# Create the align to gold table structure and add metadata

from pyspark.sql.types import *

processing_date = date_trunc('second', current_timestamp())

dataToGold = dataSilver \
    .withColumn("_keyHash", sha2(col("ZipCode").cast(StringType()), 256)) \
    .withColumn("_valueHash", sha2(concat(col("Lat"), col("Long")), 256)) \
    .withColumnRenamed("ZipCode", "zipCode") \
    .withColumnRenamed("Lat", "latitude") \
    .withColumnRenamed("Long", "longitude") \
    .withColumn("_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_processing_date", processing_date) \
    .drop("_record_modified_date")


# COMMAND ----------

# DBTITLE 1,4. Merge into Gold table
from delta.tables import *

# check if the gold table exists
if (spark.catalog.tableExists(targetTableName)):

    DeltaTable.forName(spark, targetTableName).alias("target").merge(
        source = dataToGold.alias("src"),
        condition = "src._keyHash = target._keyHash"
    ) \
    .whenMatchedUpdate(
        condition = "src._valueHash != target._valueHash",
        set = {
            "longitude" : "src.longitude",
            "latitude" : "src.latitude",
            "_pipeline_run_id" : "src._pipeline_run_id",
            "_processing_date" : "src._processing_date",
            "_valueHash" : "src._valueHash"
        }) \
    .whenNotMatchedInsert(
        values = {
            "zipCode" : "src.zipCode",
            "longitude" : "src.longitude",
            "latitude" : "src.latitude",
            "_pipeline_run_id" : "src._pipeline_run_id",
            "_processing_date" : "src._processing_date",
            "_keyHash" : "src._keyHash",
            "_valueHash" : "src._valueHash"
        }) \
    .execute()
else:
    # We do not want to automatically create the table from data frame because we want the identity column
    raise Exception(f"Table: {targetTableName} not found!")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${env}.gold_common.dimServiceLocation;
# MAGIC

# COMMAND ----------

# Maintenance for Delta table

# To optimized the performance of the Delta table, we need to execute 2 commands:
# 1. optimize(): Optimize the number of files used to store the data.
# 2. vacuum(): Remove the old version of the data. This reduces the overhead but it limites the version we can go back to. 

dataDelta = DeltaTable.forName(spark, targetTableName)

# In this example, we will run optimize and vacuum every 30 days. 
if dataDelta.history(30).filter("operation = 'VACUUM START'").count() == 0:
    dataDelta.optimize()
    dataDelta.vacuum() # Default = 7 days.