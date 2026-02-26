# Databricks notebook source
# Setup

catalog = "demo_catalog"
schema = "demo_yelp_academic"

table_namespace = f"{catalog}.{schema}"
raw_volume = f"/Volumes/{catalog}/{schema}/raw_data"


# COMMAND ----------

# Read the JSON file (implied schema) and save it as Delta table
# The goal is to keep the data 'as-is'.

# Business
df_business = spark.read.json(f"{raw_volume}/yelp_academic_dataset_business.json")
df_business.write.mode("overwrite").saveAsTable(f"{table_namespace}.bronze_business")

# Checkin
df_checkin = spark.read.json(f"{raw_volume}/yelp_academic_dataset_checkin.json")
df_checkin.write.mode("overwrite").saveAsTable(f"{table_namespace}.bronze_checkin")

# Review
df_review = spark.read.json(f"{raw_volume}/yelp_academic_dataset_review.json")
df_review.write.mode("overwrite").saveAsTable(f"{table_namespace}.bronze_review")

# Tip
df_tip = spark.read.json(f"{raw_volume}/yelp_academic_dataset_tip.json")
df_tip.write.mode("overwrite").saveAsTable(f"{table_namespace}.bronze_tip")

# User
df_user = spark.read.json(f"{raw_volume}/yelp_academic_dataset_user.json")
df_user.write.mode("overwrite").saveAsTable(f"{table_namespace}.bronze_user")

# COMMAND ----------

# Interesting fields: 
# - business_id: unique business id
# - name, address, city, state
# - Geolocation: lat and long
# - Review count, Stars

display(df_business.limit(10))

# COMMAND ----------

# Interesting fields: 
# - review_id: unique review id
# - business_id, user_id
# - date, text, stars, useful, cool, funny

display(df_review.limit(10))

# COMMAND ----------

# Interesting fields: 
# - user_id: unique user id
# - name
# - yelping_since, review_count, average_stars, elite

display(df_user.limit(10))

# COMMAND ----------

# Interesting fields: 
# - No "tips id"
# - business_id, user_id
# - date, text, compliment_count

display(df_tip.limit(10))

# COMMAND ----------

# Interesting fields: 
# - N/A

display(df_checkin.limit(10))