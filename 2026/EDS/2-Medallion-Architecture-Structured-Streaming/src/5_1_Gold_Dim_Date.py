# Databricks notebook source
# DBTITLE 1,Parameters & Configuration
# Parameters
dbutils.widgets.text("pipeline_run_id", "default_gold_run", "Pipeline Run ID")
_pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

dbutils.widgets.dropdown("reset_before_run", "false", ["true", "false"], "Reset Before Run")
reset_before_run = dbutils.widgets.get("reset_before_run")

# Configuration
catalog = "demo_catalog"
schema = "demo_yelp_academic"
target_table_prefix = "ss_gold_"
dataset = "dim_date"

# Table name
dim_date_table = f"{catalog}.{schema}.{target_table_prefix}{dataset}"

# Date range for dimension (covers your data: 2004-2022, plus buffer)
start_date = "2004-01-01"
end_date = "2025-12-31"

print(f"Date Dimension Table: {dim_date_table}")
print(f"Date Range: {start_date} to {end_date}")
print(f"Reset Before Run: {reset_before_run}")

# COMMAND ----------

# DBTITLE 1,Reset Data (Demo only)
# ** For Demo or Development/Testing only **
#   Drop table if reset flag is set

if reset_before_run == "true":
    print("\n=== RESETTING: Dropping date dimension ===")
    try:
        spark.sql(f"DROP TABLE IF EXISTS {dim_date_table}")
        print(f"  ✓ Reset {dim_date_table}")
    except Exception as e:
        print(f"  • Error: {str(e)}")
    print("=== Reset complete ===\n")
else:
    print("Skipping reset (reset_before_run=false)")

# COMMAND ----------

# DBTITLE 1,Generate Date Dimension
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

# Generate date range
start = datetime.strptime(start_date, "%Y-%m-%d")
end = datetime.strptime(end_date, "%Y-%m-%d")
date_list = [(start + timedelta(days=x),) for x in range((end - start).days + 1)]

# Create DataFrame
date_df = spark.createDataFrame(date_list, ["date_actual"])

# Add date attributes
dim_date = (date_df
    # Natural Key: Date in YYYYMMDD format
    .withColumn("NK_date_id", F.date_format(F.col("date_actual"), "yyyyMMdd").cast("int"))
    
    # Surrogate Key: Deterministic UUID from natural key (MD5 hash)
    .withColumn("SK_date_id", F.md5(F.col("NK_date_id").cast("string")))
    
    # Year attributes
    .withColumn("year", F.year(F.col("date_actual")))
    .withColumn("quarter", F.quarter(F.col("date_actual")))
    .withColumn("month", F.month(F.col("date_actual")))
    .withColumn("month_name", F.date_format(F.col("date_actual"), "MMMM"))
    .withColumn("month_abbr", F.date_format(F.col("date_actual"), "MMM"))
    
    # Week attributes
    .withColumn("week_of_year", F.weekofyear(F.col("date_actual")))
    .withColumn("day_of_week", F.dayofweek(F.col("date_actual")))  # 1=Sunday, 7=Saturday
    .withColumn("day_name", F.date_format(F.col("date_actual"), "EEEE"))
    .withColumn("day_abbr", F.date_format(F.col("date_actual"), "EEE"))
    
    # Day attributes
    .withColumn("day_of_month", F.dayofmonth(F.col("date_actual")))
    .withColumn("day_of_year", F.dayofyear(F.col("date_actual")))
    
    # Business logic flags
    .withColumn("is_weekend", F.when(F.col("day_of_week").isin(1, 7), True).otherwise(False))
    .withColumn("is_weekday", F.when(F.col("day_of_week").between(2, 6), True).otherwise(False))
    
    # Formatted date strings
    .withColumn("date_yyyymmdd", F.date_format(F.col("date_actual"), "yyyy-MM-dd"))
    .withColumn("date_mmddyyyy", F.date_format(F.col("date_actual"), "MM/dd/yyyy"))
)

print(f"Generated {dim_date.count():,} date records from {start_date} to {end_date}")
print("\nDate Dimension Schema:")
dim_date.printSchema()

print("\nSample Records:")
display(dim_date.limit(5))

# COMMAND ----------

# DBTITLE 1,Write to Gold Table
# Write date dimension as managed Delta table
(dim_date
    .write
    .format("delta")
    .mode("overwrite")  # Static dimension, safe to overwrite
    .option("overwriteSchema", "true")
    .saveAsTable(dim_date_table)
)

print(f"\n✓ Date dimension written to: {dim_date_table}")
print(f"  Mode: Overwrite (static table)")
print(f"  Format: Delta")

# COMMAND ----------

# DBTITLE 1,Verify Date Dimension
# MAGIC %sql
# MAGIC -- Verify the date dimension
# MAGIC SELECT
# MAGIC   COUNT(*) as total_dates,
# MAGIC   MIN(date_actual) as earliest_date,
# MAGIC   MAX(date_actual) as latest_date,
# MAGIC   COUNT(DISTINCT year) as unique_years,
# MAGIC   COUNT(DISTINCT quarter) as unique_quarters,
# MAGIC   SUM(CASE WHEN is_weekend THEN 1 ELSE 0 END) as weekend_days,
# MAGIC   SUM(CASE WHEN is_weekday THEN 1 ELSE 0 END) as weekdays
# MAGIC FROM demo_catalog.demo_yelp_academic.ss_gold_dim_date