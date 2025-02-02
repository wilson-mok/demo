# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Load the [env].bronze_iotSmartGrid.powerMeter based on the bronze_processing_date parameter
# MAGIC 1. Correct any schema issues
# MAGIC 1. Merge the changes into the silver zone and create a delta table called [env].silver_operation.powerMeter

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1021")

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "silver"

# Define the dataset details and location
businessDomain = "operation"
datasetName = "iotPowerMeter"

# Src - Table
srcTableName = f"{env}.bronze_iotSmartGrid.powerMeter"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"


# COMMAND ----------

# retrieve the data that has been added today. 

from pyspark.sql.functions import *

dataBronze = spark.read.table(srcTableName)

# COMMAND ----------

# DBTITLE 1,1. Data Quality check
from pyspark.sql.functions import *

# Class = Small, Medium, Large
dataDQClean = dataBronze.filter("class IS NOT NULL AND class IN ('Small', 'Medium', 'Large')")

dataDqError = dataBronze.subtract(dataDQClean)


# COMMAND ----------

# DBTITLE 1,2. Data Deduplication
from pyspark.sql.functions import *
from pyspark.sql import *

dataWindowSpec = Window.partitionBy("meterId").orderBy(col("_input_file_modification_date").desc())
findLatest = dataDQClean.withColumn("row_number", row_number().over(dataWindowSpec)).filter("row_number = 1").drop("row_number")

# COMMAND ----------

# Row count check

print("DQ Error:", dataDqError.count())
print("Clean data:", dataDQClean.count())
print("Latest data:", findLatest.count())

# COMMAND ----------

# DBTITLE 1,3. Correct Data Structure issue and add metadata
# Create the silver metadata
from pyspark.sql.types import *

processing_date = date_trunc('second', current_timestamp())

findLatest = findLatest \
    .withColumn("MeterNumber", col("MeterNumber").cast(IntegerType())) \
    .withColumn("maxKwh", col("maxKwh").cast(DecimalType(10,4))) \
    .withColumn("efficiencyPercentage", col("efficiencyPercentage").cast(DecimalType())) \
    .withColumn("startupTimeMin", col("startupTimeMin").cast(IntegerType()))

# Replace whole number percentage with decimal
findLatest = findLatest.withColumn("efficiencyPercentage", col("efficiencyPercentage")/100)

dataToSilver = findLatest \
    .withColumnRenamed("_input_file_modification_date", "_record_modified_date") \
    .withColumn("_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_processing_date", processing_date) \
    .drop("_input_filename")


# COMMAND ----------

# DBTITLE 1,4. Merge into Silver table
from delta.tables import *

# check if the silver table exists
if (spark.catalog.tableExists(targetTableName)):

    DeltaTable.forName(spark, targetTableName).alias("target").merge(
        source = dataToSilver.alias("src"),
        condition = "src.meterId = target.meterId"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll().execute()
else:
    tempName = f"_{env}_{targetDataZone}_{businessDomain}_{datasetName}"
    dataToSilver.createOrReplaceTempView(tempName)
    spark.sql(f"CREATE EXTERNAL TABLE {targetTableName} LOCATION '{targetLocation}' AS SELECT * FROM {tempName}")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${env}.silver_operation.iotPowerMeter;

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