# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Create the [env].silver_production.iotPowerConsumption external table using a defined schema. 
# MAGIC 1. Load the [env].bronze_iotSmartGrid_powerConsumption UC external table.
# MAGIC 1. Using structure streaming to load any new records into Silver zone.
# MAGIC 1. This script will correct any schema issue and conduct a data quality check prior loading the data into Silver.

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1121")

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "silver"

# Define the dataset details and location
businessDomain = "operation"
datasetName = "iotPowerConsumption"

# Src - Table
srcTableName = f"{env}.bronze_iotSmartGrid.powerConsumption"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"

checkpointsLocation = f"{targetLocation}/_checkpoints"

# COMMAND ----------

# create and assign the permission for the catalog, schema and table, if required.

spark.sql(f"""
          CREATE EXTERNAL TABLE IF NOT EXISTS {targetTableName} (
              measurementDate timestamp,
              measurementInKWh decimal(20,3),
              meterId string,
              zipCode bigint,
              _record_modified_date timestamp,
              _pipeline_run_id string,
              _processing_date timestamp)
          USING delta LOCATION '{targetLocation}' 
          """)

# COMMAND ----------

# For Demo only, lets look at our Bronze data
demoDf = spark.read.table(srcTableName)
demoDf.printSchema()

display(demoDf.limit(10))

# COMMAND ----------

# DBTITLE 1,1. Data Quality function
from pyspark.sql.functions import *
from pyspark.sql import *

def dataQualityCheck(batchDf):

    # Going to skip all the data that does not matched the bronze schema
    cleanDf = batchDf.filter("_rescued_data IS NULL")

    # The kWh must be between 5 to 20 kWh
    cleanDf = cleanDf.filter("measurementInKWh IS NOT NULL AND measurementInKWh between 5.0 AND 20.0")

    # The measure date value must be a timestamp
    cleanDf = cleanDf.withColumn("measurementDate_ts", to_timestamp("measurementDate")).filter("measurementDate_ts IS NOT NULL").drop("measurementDate_ts")

    dqErrorDf = batchDf.subtract(cleanDf)

    return (cleanDf, dqErrorDf)

# COMMAND ----------

# DBTITLE 1,2. Data Deduplication function
# Our data is transactional. This means for a specific date/time a power meter can only have one value for Kwh. 
# In a real world scenario, the data might need to be corrected due to equipment error or system delays.
# Because of this, we will only consider the newest record as the correct value.

# We are not using dropDuplicate function because it does not allow us to grab the newest record.

def dataDeduplication(batchDf):

    gridDataWindowSpec = Window.partitionBy("measurementDate", "meterId", "zipCode").orderBy(col("_input_file_modification_date").desc(), "measurementInKWh")
    batchLatest = batchDf.withColumn("row_number", row_number().over(gridDataWindowSpec)).filter("row_number = 1").drop("row_number")

    return batchLatest

# COMMAND ----------

# DBTITLE 1,3. Correct Data structure and add metadata
from pyspark.sql.types import *

def correctSchemaWithMetadata(batchDf):

    findLatest = batchDf \
        .withColumn("measurementDate", to_timestamp("measurementDate")) \
        .withColumn("measurementInKWh", col("measurementInKWh").cast(DecimalType(20,15)))

    dataToSilver = findLatest \
        .withColumnRenamed("_input_file_modification_date", "_record_modified_date") \
        .withColumn("_pipeline_run_id", col("_silver_pipeline_run_id")) \
        .withColumn("_processing_date", col("_silver_processing_date")) \
        .drop("_silver_pipeline_run_id") \
        .drop("_silver_processing_date") \
        .drop("_input_filename") \
        .drop("_rescued_data")

    return dataToSilver

# COMMAND ----------

# In this demo, we will write out the clean data only.
# For a production workload, you want to write the data with DQ error to a different location for troubleshooting purposes.

from delta.tables import *

def wrapperProcess(microBatchDf, batchId):

    dqCheck = dataQualityCheck(microBatchDf)
    cleanDataDf = dataDeduplication(dqCheck[0])
    dataToSilver = correctSchemaWithMetadata(cleanDataDf)

    dqErrorDf = dqCheck[1]
    
    print(f"Micro-batch ({batchId}): Clean Data ({cleanDataDf.count()}), DQ Error ({dqErrorDf.count()})")

    # Again, this data is transactional, so no history is kept. 
    DeltaTable.forName(spark,targetTableName).alias("target").merge(
        source = dataToSilver.alias("src"),
        condition = "target.measurementDate = src.measurementDate and target.meterId = src.meterId and target.zipCode = src.zipCode"
    ) \
    .whenMatchedUpdate(
        condition = "src._record_modified_date > target._record_modified_date",
        set = {
            "measurementInKWh" : "src.measurementInKWh",
            "_record_modified_date" : "src._record_modified_date",
            "_pipeline_run_id": "src._pipeline_run_id",
            "_processing_date": "src._processing_date",
        }
    ).whenNotMatchedInsertAll().execute()

# COMMAND ----------


# For demo purpopses, I limit the max files per trigger to 10 so we can have multiple batches
# Read the bronze data using structure streaming

bronzeStream  = spark.readStream \
    .option("maxFilesPerTrigger", "10") \
    .table(srcTableName)

# COMMAND ----------

from pyspark.sql.functions import *

newDataStream = bronzeStream.withColumn("_silver_pipeline_run_id", lit(dbutils.widgets.get('_pipeline_run_id'))) \
    .withColumn("_silver_processing_date", date_trunc('second', current_timestamp()))

# COMMAND ----------

# We are overriding the default writing behaviour. 
# The "foreachBatch" function will call the wrapper process
#  and the wrapper process needs to write each batch into Silver. 

newDataStream.writeStream \
    .foreachBatch(wrapperProcess) \
    .trigger(availableNow=True) \
    .option("checkpointLocation", checkpointsLocation) \
    .option("mergeSchema", "true")  \
    .outputMode("append") \
    .queryName(f"stream-{targetTableName}") \
    .start().awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM ent_dev.silver_operation.iotpowerconsumption;

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
# MAGIC SELECT *
# MAGIC FROM ${env}.silver_operation.iotPowerConsumption
# MAGIC WHERE measurementDate = '2023-01-01T11:30:00'
# MAGIC   and meterId = '98027-4'
# MAGIC   and zipCode = 98027
# MAGIC ORDER BY measurementDate, meterId, zipCode