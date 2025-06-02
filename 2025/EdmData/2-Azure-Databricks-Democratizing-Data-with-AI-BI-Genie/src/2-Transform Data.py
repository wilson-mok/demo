# Databricks notebook source
from pyspark.sql.functions import *

# Setup namespace
table_yelp_namespace = "demo_catalog.demo_yelp_academic"

# COMMAND ----------

# business
df_business_bronze = spark.table(f"{table_yelp_namespace}.bronze_business")

df_business_clean = df_business_bronze.select(
    "business_id", 
    "name",
    "city",
    "state",
    "latitude",
    "longitude",
    "stars",
    "review_count",
    "categories",
    expr("try_cast(attributes.RestaurantsPriceRange2 as int)").alias("priceRange"),
    expr("try_cast(attributes.WheelchairAccessible AS BOOLEAN)").alias("wheelchairAccessible"),
    expr("try_cast(attributes.DriveThru AS BOOLEAN)").alias("driveThru"),
    expr("try_cast(attributes.RestaurantsTakeOut AS BOOLEAN)").alias("takeOut"),
    translate(col("attributes.WiFi"), "u'", "").alias("wifi")
)

df_business_clean = df_business_clean.withColumn("freeWifi", 
                        when(col("wifi") == 'free', True)
                            .when(col("wifi") == 'paid', False)
                            .otherwise(None)
          ) \
          .withColumn("categories", split(trim(col("categories")), ",\\s*"))

df_business_silver = df_business_clean.drop("wifi")
            
df_business_silver.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.silver_business")

# COMMAND ----------

# user
df_user_bronze = spark.table(f"{table_yelp_namespace}.bronze_user")

display(df_user_bronze.limit(10))

# COMMAND ----------

# user
df_user_bronze = spark.table(f"{table_yelp_namespace}.bronze_user")

df_user_clean = df_user_bronze.select(
    "user_id",
    "name",
    to_date(col("yelping_since"), "yyyy-MM-dd HH:mm:ss").alias("member_since"),
    "review_count",
    col("cool").alias("review_cool_votes"),
    col("useful").alias("review_useful_votes"),
    col("funny").alias("review_funny_votes"),
    "average_stars",
    when(trim(col("elite")) == "", None).otherwise(split(trim(col("elite")), ",\\s*")).alias("elite_years"),
    when(trim(col("elite")) == "", 0).otherwise(size(split(trim(col("elite")), ",\\s*"))).alias("elite_year_count")
)

df_user_silver = df_user_clean
            
df_user_silver.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.silver_user")

# COMMAND ----------

# review
df_review_bronze = spark.table(f"{table_yelp_namespace}.bronze_review")

df_review_clean = df_review_bronze.select(
    "review_id",
    "user_id",
    "business_id",
    to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("review_date"),
    "stars",
    col("useful").alias("review_useful_votes"),
    col("funny").alias("review_funny_votes"),
    col("cool").alias("review_cool_votes"),
    "text"
)

df_review_silver = df_review_clean
            
df_review_silver.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.silver_review")

# COMMAND ----------

# tip
df_tip_bronze = spark.table(f"{table_yelp_namespace}.bronze_tip")

df_tip_clean = df_tip_bronze.select(
    "user_id",
    "business_id",
    to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("tip_date"),
    "text",
    "compliment_count",
    when(col("compliment_count") >= 3, True).otherwise(False).alias("is_popular_tip")
)

display(df_tip_clean)

df_tip_silver = df_tip_clean
            
df_tip_silver.write.mode("overwrite").saveAsTable(f"{table_yelp_namespace}.silver_tip")