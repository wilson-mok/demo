# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Retrieve the Zip Code - Geo Location data (CSV file) from the Data Lake (landing volume)
# MAGIC 1. Ingest the data
# MAGIC 1. Load the data into the bronze zone and create an external delta table named [env].bronze_corpSharedDrive.zipCodeGeoLocation.
# MAGIC 1. Append the new data into the [env].bronze_corpSharedDrive.zipCodeGeoLocation table.
# MAGIC

# COMMAND ----------

# Create the landing_folder parameter.

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1012")

# COMMAND ----------

# Get the external location.

extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

display(extLocDf)

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location

# Retrieve from parameters
env = dbutils.widgets.get('env')
rawLocation = f"/Volumes/{env}/landing/raw"
targetDataZone = "bronze"

# Define the dataset details and location
srcSystem = "corpSharedDrive"
datasetName = "zipCodeGeoLocation"

# Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

srcLocation = f"{rawLocation}/{datasetName}/*.csv"
targetLocation = f"{extLocUrl}{srcSystem}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{srcSystem}.{datasetName}"

# COMMAND ----------

# Read the Zip Code - Geo location CSV data from the Data Lake
zipCodeGeoLocationDf = spark.read.option("header",True).csv(srcLocation)

# COMMAND ----------

from pyspark.sql.functions import *

# Add the audit columns to the gridData
# 1. _pipeline_run_id: The pipeline run id from ADF.
# 2. _processing_date: The current datetime to process this dataset.
# 3. _input_filename: The landing filename. This is very useful for debugging purposes.
# 4. _input_file_modification_date: This date helps identified order of data when the dataset does not have a modification date.

processing_date = date_trunc('second', current_timestamp())

zipCodeGeoLocationDf = zipCodeGeoLocationDf \
    .withColumn("_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_processing_date", processing_date) \
    .withColumn("_input_filename", col("_metadata.file_path")) \
    .withColumn("_input_file_modification_date", col("_metadata.file_modification_time"))
    

# COMMAND ----------

if (spark.catalog.tableExists(targetTableName)):
    zipCodeGeoLocationDf.write.mode("append").save(targetLocation)
else:
    tempName = f"_{env}_{targetDataZone}_{srcSystem}_{datasetName}"
    zipCodeGeoLocationDf.createOrReplaceTempView(tempName)
    spark.sql(f"CREATE EXTERNAL TABLE {targetTableName} LOCATION '{targetLocation}' AS SELECT * FROM {tempName}")
    

# COMMAND ----------

display(spark.sql(f"SELECT count(*) FROM {targetTableName}"))