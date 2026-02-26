# Databricks notebook source
# credit: https://business.yelp.com/data/resources/open-dataset/

raw_yelp_volume = "/Volumes/demo_catalog/demo_yelp_academic/raw_data"
table_yelp_namespace = "demo_catalog.demo_yelp_academic"

# COMMAND ----------

df_business = spark.read.json(f"{raw_yelp_volume}/yelp_academic_dataset_business.json")
df_business.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.bronze_business")

df_checkin = spark.read.json(f"{raw_yelp_volume}/yelp_academic_dataset_checkin.json")
df_checkin.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.bronze_checkin")

df_review = spark.read.json(f"{raw_yelp_volume}/yelp_academic_dataset_review.json")
df_review.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.bronze_review")

df_tip = spark.read.json(f"{raw_yelp_volume}/yelp_academic_dataset_tip.json")
df_tip.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.bronze_tip")

df_user = spark.read.json(f"{raw_yelp_volume}/yelp_academic_dataset_user.json")
df_user.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.bronze_user")

# COMMAND ----------

display(df_business.limit(10))

# COMMAND ----------

display(df_checkin.limit(10))

# COMMAND ----------

display(df_review.limit(10))

# COMMAND ----------

display(df_tip.limit(10))

# COMMAND ----------

display(df_user.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Genie Questions
# MAGIC
# MAGIC 1. We have nested data in JSON (very common), can Genie handle it? For example, Wheelchair Accessible is very important to me. 