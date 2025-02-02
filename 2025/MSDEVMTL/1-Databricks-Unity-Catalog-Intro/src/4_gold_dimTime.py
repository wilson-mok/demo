# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Create the date attributes for the dimDate in the gold layer.
# MAGIC 1. Merge the changes into the gold delta table called [env].gold_common.dimTime.
# MAGIC

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1032")

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "gold"

# Define the dataset details and location
businessDomain = "common"
datasetName = "dimTime"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# In Databricks, we do not have a TimeType, so we cannot store the time directly.

baseDf = spark.createDataFrame([(1,)], ["id"])

timeDf = baseDF.withColumn(
    "timestamp", 
    explode(expr(f"""sequence(to_timestamp('2022-01-01T00:00:00'), to_timestamp('2022-01-01T23:59:59'), interval 1 seconds)"""))
).drop("id")

dimTimeDf = timeDF.withColumn("sid", date_format(col("timestamp"), "HHmmss")) \
        .withColumn("time", date_format(col("timestamp"), "HH:mm:ss")) \
        .withColumn("hour", hour(col("timestamp"))) \
        .withColumn("minute", minute(col("timestamp"))) \
        .withColumn("second", second(col("timestamp"))).drop("timestamp")

dimTimeDf = dimTimeDF.withColumn("dayPart", when(col("hour").between(0,5), 'Night')
                                .when(col("hour").between(6,11), 'Morning')
                                .when(col("hour").between(12,17), 'Afternoon')
                                .otherwise('Evening'))


# COMMAND ----------

from delta.tables import *

# check if the silverLocation contain the delta table
if (spark.catalog.tableExists(targetTableName)):

    # Again, this data is transactional, so no history is kept. 
    # If the same value is being addded 
    DeltaTable.forName(spark, targetTableName).alias("target").merge(
        source = dimTimeDf.alias("src"),
        condition = "target.sid = src.sid"
    ) \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    tempName = f"_{env}_{targetDataZone}_{businessDomain}_{datasetName}"
    dimTimeDf.createOrReplaceTempView(tempName)
    spark.sql(f"CREATE EXTERNAL TABLE {targetTableName} LOCATION '{targetLocation}' AS SELECT * FROM {tempName}")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${env}.gold_common.dimTime;