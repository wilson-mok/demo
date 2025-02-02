# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Create the [env].gold_operation.factPowerConsumption using a defined schema. 
# MAGIC 1. Load the [env].silver_operation.iotPowerConsumption table.
# MAGIC 1. Using structure streaming to transform the new records into a 'Fact' table structure.
# MAGIC 1. All the changes will be merged into the gold external table.
# MAGIC

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1131")


# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "gold"

# Define the dataset details and location
businessDomain = "operation"
datasetName = "factPowerConsumption"

# Src - Table
srcTableName = f"{env}.silver_operation.iotPowerConsumption"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"

checkpointsLocation = f"{targetLocation}/_checkpoints"

# COMMAND ----------

# create table, if required.

spark.sql(f"""
          CREATE EXTERNAL TABLE IF NOT EXISTS {targetTableName} (
              sid bigint GENERATED ALWAYS AS IDENTITY (START WITH 0 INCREMENT BY 1),
              meterSid bigint,
              serviceLocationSid bigint,
              measurementDateSid string,
              measurementTimeSid string,
              powerConsumptedKwh decimal(20,3),
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

# Dim tables for the star schema
dimDate = f"{env}.{targetDataZone}_common.dimDate"
dimTime = f"{env}.{targetDataZone}_common.dimTime"
dimServiceLocation = f"{env}.{targetDataZone}_common.dimServiceLocation"
dimPowerMeter = f"{env}.{targetDataZone}_operation.dimPowerMeter"

# COMMAND ----------

# DBTITLE 1,1. Find the matching SID with the Dim tables
from pyspark.sql.functions import *

# This function will replace the column value with the corrsponding Dim table's SID. 
# The other dim tables can be either a streaming or batch

def buildDimTableSid(batchDf):
        # Since date and time are 2 seperate Dimensions, lets split the measurementDate into 2 temprary columns
        mappingToDims = batchDf \
                .withColumn("tempDate", to_date(col("measurementDate"))) \
                .withColumn("tempTime", date_format(col("measurementDate"), "HH:mm:ss"))

        # Bring in the dimension tables
        dataDimDate = batchDf.sparkSession.read.table(dimDate).select("calendarDate", col("sid").alias("measurementDateSid"))
        dataDimTime = batchDf.sparkSession.read.table(dimTime).select("time", col("sid").alias("measurementTimeSid"))
        dataDimPowerMeter = batchDf.sparkSession.read.table(dimPowerMeter).select("meterId", col("sid").alias("meterSid"))
        dataDimServiceLocation = batchDf.sparkSession.read.table(dimServiceLocation).select("zipCode", col("sid").alias("serviceLocationSid"))

        # Join all the data to find the matching SID
        mappedData = mappingToDims \
        .join(dataDimDate, mappingToDims.tempDate == dataDimDate.calendarDate, how="left") \
        .join(dataDimTime, mappingToDims.tempTime == dataDimTime.time, how="left") \
        .join(dataDimPowerMeter, mappingToDims.meterId == dataDimPowerMeter.meterId, how="left") \
        .join(dataDimServiceLocation, mappingToDims.zipCode == dataDimServiceLocation.zipCode, how="left")

        mappedData = mappedData.select("meterSid", "serviceLocationSid", "measurementDateSid", "measurementTimeSid", "measurementInKWh", "_gold_pipeline_run_id", "_gold_processing_date")

        # if any unmatch, the value = -1.
        #  Alternatively, you can return the match and unmatched data into different data frame. 
        mappedData = mappedData \
                .withColumn("meterSid", when(col("meterSid").isNull(), lit(-1)).otherwise(col("meterSid"))) \
                .withColumn("serviceLocationSid", when(col("serviceLocationSid").isNull(), lit(-1)).otherwise(col("serviceLocationSid"))) \
                .withColumn("measurementDateSid", when(col("measurementDateSid").isNull(), lit(-1)).otherwise(col("measurementDateSid"))) \
                .withColumn("measurementTimeSid", when(col("measurementTimeSid").isNull(), lit(-1)).otherwise(col("measurementTimeSid")))

        return mappedData

# COMMAND ----------

# DBTITLE 1,2. Correct Data structure and add metadata
from pyspark.sql.types import *

# Generate the standard metadata column, keyHash and valueHash.
# Rename the Kwh column as well.

def correctSchemaWithMetadata(batchDf):
    dataToGold = batchDf \
        .withColumn("_keyHash", sha2(concat(col("meterSid"), col("serviceLocationSid"), col("measurementDateSid"), col("measurementTimeSid")), 256)) \
        .withColumn("_valueHash", sha2(concat(col("measurementInKWh")), 256)) \
        .withColumnRenamed("measurementInKWh","powerConsumptedKwh") \
        .withColumn("_pipeline_run_id", col("_gold_pipeline_run_id")) \
        .withColumn("_processing_date", col("_gold_processing_date")) \

    return dataToGold

# COMMAND ----------

from delta.tables import *

# We are using the keyHash and valueHash to determine when a record needs to be inserted or updated.
# Note: We are not specifying the identity column in the update or insert.

def wrapperProcess(microBatchDf, batchId):
    mapToDimsDf = buildDimTableSid(microBatchDf)
    dataToGold = correctSchemaWithMetadata(mapToDimsDf)

    print(f"Micro-batch ({batchId}): Data ({dataToGold.count()})")
    
    if (spark.catalog.tableExists(targetTableName)):
        DeltaTable.forName(spark,targetTableName).alias("target").merge(
            source = dataToGold.alias("src"),
            condition = "target._keyHash = src._keyHash"
        ) \
        .whenMatchedUpdate(
            condition = "target._valueHash != src._valueHash",
            set = {
                "powerConsumptedKwh" : "src.powerConsumptedKwh",
                "_pipeline_run_id" : "src._pipeline_run_id",
                "_processing_date" : "src._processing_date",
                "_valueHash" : "src._valueHash"
            }
        ) \
        .whenNotMatchedInsert(
            values = {
                "meterSid" : "src.meterSid",
                "serviceLocationSid" : "src.serviceLocationSid",
                "measurementDateSid" : "src.measurementDateSid",
                "measurementTimeSid" : "src.measurementTimeSid",
                "powerConsumptedKwh" : "src.powerConsumptedKwh",
                "_pipeline_run_id" : "src._pipeline_run_id",
                "_processing_date" : "src._processing_date",
                "_keyHash" : "src._keyHash",
                "_valueHash" : "src._valueHash"
            }
        ) \
        .execute()
    else:
        # We do not want to automatically create the table from data frame because we want the identity column
        raise Exception(f"Delta table: {targetTableName} not found!")


# COMMAND ----------

from pyspark.sql.functions import *

silverStream = spark.readStream.table(srcTableName)
newDataStream = silverStream.withColumn("_gold_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_gold_processing_date", date_trunc('second', current_timestamp()))


# COMMAND ----------

# Lets trigger the stream



newDataStream.writeStream \
    .foreachBatch(wrapperProcess) \
    .trigger(availableNow=True) \
    .option("mergeSchema", "true")  \
    .outputMode("append") \
    .option("checkpointLocation", checkpointsLocation) \
    .queryName(f"stream-{targetTableName}") \
    .start().awaitTermination()

# COMMAND ----------

# Maintenance for Delta table

# To optimized the performance of the Delta table, we need to execute 2 commands:
# 1. optimize(): Optimize the number of files to store the data.
# 2. vacuum(): Remove the old version of the data. This reduces the overhead but it limites the version we can go back to. 

# By having the "awaitTermination()" on the writeStream, we will run the maintenance at the end.

from delta.tables import *

dataDeltaDf = DeltaTable.forName(spark, targetTableName)

# In this example, we will run optimize and vacuum every 30 days. 
if dataDeltaDf.history(30).filter("operation = 'VACUUM START'").count() == 0:
    dataDeltaDf.optimize()
    dataDeltaDf.vacuum() # Default = 7 days.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Since the processing can be distributed, the sid is not guarantee to have consecutive value.
# MAGIC -- By using the identity, it does guarantee uniqueness. 
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${env}.gold_operation.factPowerConsumption;