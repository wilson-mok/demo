# Databricks notebook source
dataLakeName = "staadbdemoentdatadev"

locations = [
    f"abfss://bronze@{dataLakeName}.dfs.core.windows.net/", 
    f"abfss://silver@{dataLakeName}.dfs.core.windows.net/",
    f"abfss://gold@{dataLakeName}.dfs.core.windows.net/",
    ]

# COMMAND ----------

# Clean up files

for file_location in locations:
    dbutils.fs.rm(file_location, True)