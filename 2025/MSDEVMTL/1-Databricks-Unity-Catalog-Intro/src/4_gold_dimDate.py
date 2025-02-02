# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC This code will: 
# MAGIC 1. Create the date attributes for the dimDate in the gold layer.
# MAGIC 1. Merge the changes into the gold delta table called [env].gold_common.dimDate.
# MAGIC

# COMMAND ----------

# create parameters

dbutils.widgets.text("env", "ent_dev")
dbutils.widgets.text("_pipeline_run_id", "1031")

# Special parameters for dimDate only
dbutils.widgets.text("startDate", "2023-01-01")
dbutils.widgets.text("endDate", "2023-12-31")

# COMMAND ----------

# We dont have to hard code the location path now. We can retrieve it from Volume and external location
extLocDf = spark.sql("SHOW EXTERNAL LOCATIONS")

# Retrieve from parameters
env = dbutils.widgets.get('env')
targetDataZone = "gold"

# Define the dataset details and location
businessDomain = "common"
datasetName = "dimDate"

# Target - Location and Table
extLocName = f"ext_loc_{env}_{targetDataZone}"
extLocUrl = extLocDf.select("url").filter(f"name = '{extLocName}'").first()[0]

targetLocation = f"{extLocUrl}{businessDomain}/{datasetName}"
targetTableName = f"{env}.{targetDataZone}_{businessDomain}.{datasetName}"


# COMMAND ----------

from pyspark.sql.functions import *

baseDf = spark.createDataFrame([(1,)], ["id"])

dateDf = baseDF.withColumn(
    "calendarDate", 
    explode(expr(f"""sequence(to_date('{dbutils.widgets.get('startDate')}'), to_date('{dbutils.widgets.get('endDate')}'), interval 1 day)"""))
).drop("id")

# dayofweek = 1 - Sunday 
dimDateDf = dateDF.withColumn("sid", date_format(col("calendarDate"), "yyyyMMdd")) \
        .withColumn("year", year(col("calendarDate"))) \
        .withColumn("month", month(col("calendarDate"))) \
        .withColumn("dayofmonth", dayofmonth(col("calendarDate"))) \
        .withColumn("dayofweek", dayofweek(col("calendarDate"))) \
        .withColumn("quarter", quarter(col("calendarDate")))

# COMMAND ----------

from delta.tables import *

# check if the silverLocation contain the delta table
if (spark.catalog.tableExists(targetTableName)):

    # Again, this data is transactional, so no history is kept. 
    # If the same value is being addded 
    DeltaTable.forName(spark, targetTableName).alias("target").merge(
        source = dimDateDf.alias("src"),
        condition = "target.sid = src.sid"
    ) \
    .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    tempName = f"_{env}_{targetDataZone}_{businessDomain}_{datasetName}"
    dimDateDf.createOrReplaceTempView(tempName)
    spark.sql(f"CREATE EXTERNAL TABLE {targetTableName} LOCATION '{targetLocation}' AS SELECT * FROM {tempName}")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM ${env}.gold_common.dimDate;