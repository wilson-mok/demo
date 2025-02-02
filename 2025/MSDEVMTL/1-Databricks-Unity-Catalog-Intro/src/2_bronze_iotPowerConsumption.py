# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Retrieve the power consumption files (JSON format) from the Data Lake (landing volume)
# MAGIC 1. Using Autoloader, this script will read the JSON files as a 'readStream'
# MAGIC 1. Save the data into the bronze zone using micro-batching.
# MAGIC 1. Create a delta table named [env].bronze_iotSmartGrid.powerConsumption.
# MAGIC 1. Append the new data into the powerConsumption table.
# MAGIC

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1111")

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
rawLocation = f"/Volumes/{env}/landing/raw"
targetDataZone = "bronze"

# Define the dataset details and location
srcSystem = "iotSmartGrid"
datasetName = "powerConsumption"

# Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

srcLocation = f"{rawLocation}/iotSmartGridData/*/*/*/*/*.json"
targetLocation = f"{extLocUrl}{srcSystem}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{srcSystem}.{datasetName}"

inferSchemaLocation = f"{targetLocation}/_landingInferSchema"
checkpointsLocation = f"{targetLocation}/_checkpoints"

# COMMAND ----------

# Demo only - Lets take a look at our data. 

demoDf = spark.read.json(srcLocation)

demoDf.printSchema()
display(demoDf.limit(10))

# The measurementDate is a string intead of a timestamp. We need to correct this when going from Bronze to Silver.

# COMMAND ----------

# Using Auto Loader to read new files in for processing 

# Demo purpose only 
# - maxFilesPerTrigger. This will read 75 files for each batch. 
# - For your workload, consider using maxBytesPerTrigger instead. 
# - Note: By default, JSON file is inferred to string data type.

srcStream = (spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.maxFilesPerTrigger", "75") \
    .option("cloudFiles.inferColumnTypes", "true") \
    .option("cloudFiles.schemaLocation", inferSchemaLocation) \
    .load(srcLocation))

# With Streaming, it is important to not run 'display()' on the readStream. 
#  This results in the process continue to search for new files and the notebook will continue to run.

# COMMAND ----------

# Similar to the spark.read, you have access to the _metadata information
# 1. _input_filename: The field stores filename of the landing file. This is very useful for debugging purposes.
# 2. _input_file_modification_date: This date helps identified order of data when the dataset does not have a modification date.

# Additionally, we want to add 2 addditional metadata into our dataset: 
# 1. _pipeline_run_id: The pipeline run id from ADF.
# 2. _processing_date: The current datetime to process this dataset.


from pyspark.sql.functions import *

newDataStream = srcStream.withColumn("_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_processing_date", date_trunc('second', current_timestamp())) \
    .withColumn("_input_filename", col("_metadata.file_path")) \
    .withColumn("_input_file_modification_date", col("_metadata.file_modification_time"))
    

# COMMAND ----------

# Process all the available data in micro-batches.

newDataStream.writeStream \
    .format("delta") \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpointsLocation) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .queryName(f"stream-{targetTableName}") \
    .start(targetLocation).awaitTermination()

# COMMAND ----------

# create and assign the permission for the catalog, schema and table, if required.
spark.sql(f"CREATE EXTERNAL TABLE IF NOT EXISTS {targetTableName} LOCATION '{targetLocation}'")

# COMMAND ----------

# A '_rescued_data' column is automatically created. If the data does not match the inferSchema, the data will be stored in this column instead. 
#   A common approach is to write the rescued data into a different location for troubleshooting. 

display(spark.sql(f"SELECT * FROM {targetTableName}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) cnt
# MAGIC FROM identifier(:env || ".bronze_iotSmartGrid.powerConsumption");