# Databricks notebook source
# DBTITLE 1,Data Analysis: Yelp Academic Dataset
# MAGIC %md
# MAGIC # Yelp Academic Dataset Analysis
# MAGIC
# MAGIC This notebook provides comprehensive analysis of the Yelp Academic Dataset stored in `/Volumes/demo_catalog/demo_yelp_academic/raw_data/`
# MAGIC
# MAGIC ## Datasets Overview
# MAGIC
# MAGIC 1. **Business**: Business information (location, hours, attributes, ratings)
# MAGIC 2. **Checkin**: Check-in timestamps for businesses
# MAGIC 3. **Review**: User reviews with ratings and text
# MAGIC 4. **Tip**: Quick tips/suggestions from users
# MAGIC 5. **User**: User profiles and engagement metrics

# COMMAND ----------

# DBTITLE 1,Load All Datasets
# Load all datasets
business_df = spark.read.json("/Volumes/demo_catalog/demo_yelp_academic/raw_data/business/")
checkin_df = spark.read.json("/Volumes/demo_catalog/demo_yelp_academic/raw_data/checkin/")
review_df = spark.read.json("/Volumes/demo_catalog/demo_yelp_academic/raw_data/review/")
tip_df = spark.read.json("/Volumes/demo_catalog/demo_yelp_academic/raw_data/tip/")
user_df = spark.read.json("/Volumes/demo_catalog/demo_yelp_academic/raw_data/user/")

print("✓ All datasets loaded successfully")

# COMMAND ----------

# DBTITLE 1,Dataset Size Overview
# Dataset size overview
from pyspark.sql.functions import col, count, countDistinct

dataset_stats = [
    ("Business", business_df.count(), len(business_df.columns)),
    ("Checkin", checkin_df.count(), len(checkin_df.columns)),
    ("Review", review_df.count(), len(review_df.columns)),
    ("Tip", tip_df.count(), len(tip_df.columns)),
    ("User", user_df.count(), len(user_df.columns))
]

stats_df = spark.createDataFrame(dataset_stats, ["Dataset", "Record_Count", "Column_Count"])
display(stats_df)

# COMMAND ----------

# DBTITLE 1,Business Analysis Section
# MAGIC %md
# MAGIC ## Business Dataset Analysis

# COMMAND ----------

# DBTITLE 1,Business Schema and Sample
# Business dataset schema and sample
print("=== Business Dataset Schema ===")
business_df.printSchema()

print("\n=== Sample Records ===")
display(business_df.select("business_id", "name", "city", "state", "stars", "review_count", "is_open").limit(10))

# COMMAND ----------

# Deeper analysis for Alberta (AB) or city similar to 'Edmonton'
from pyspark.sql.functions import lower, col, count, avg

# Filter for state = 'AB' or city contains 'edmonton' (case-insensitive, handles spelling variations)
filtered_business = business_df.filter(
    (col("state") == "AB") |
    (lower(col("city")).like("%edmont%"))
)

# Top cities in Alberta/Edmonton-like, show state as well
top_cities = filtered_business.groupBy("city", "state") \
    .agg(count("*").alias("business_count"),
         avg("stars").alias("avg_stars")) \
    .orderBy(col("business_count").desc())

display(top_cities)

# COMMAND ----------

# DBTITLE 1,Data Quality Check 1: City Name
# DQ Check 1: Check different City name in AB.
# - Different spelling of Edmonton, St. Albert and Sherwood Park
# - The standardize city name does not solve all the problems, but it is a start.

from pyspark.sql.functions import lower, when, col

ab_business_df = business_df.filter(col("state") == "AB")

normalized_ab_business_df = ab_business_df.withColumn(
    "city_standardized",
    when(lower(col("city")).like("%edmont%"), "Edmonton")
    .when(lower(col("city")).rlike("st[\\s\\.]*alb|saint alb|staint alb"), "St. Albert")
    .when(lower(col("city")).rlike("sherwood park|sherwood"), "Sherwood Park")
    .otherwise(col("city"))
)

display(
    normalized_ab_business_df
        .select("city", "city_standardized", "state")
        .distinct()
        .orderBy("city_standardized")
)

# COMMAND ----------

# DBTITLE 1,Data Quality 2: Rating
# DQ Check 1: Use Business statistics to confirm if we have any null ratings
# - rating returns 1 - 5. No issue found

from pyspark.sql.functions import col, avg, min, max, count, when

business_stats = business_df.select(
    count("*").alias("total_businesses"),
    countDistinct("city").alias("unique_cities"),
    countDistinct("state").alias("unique_states"),
    avg("stars").alias("avg_rating"),
    min("stars").alias("min_rating"),
    max("stars").alias("max_rating"),
    avg("review_count").alias("avg_review_count"),
    count(when(col("is_open") == 1, True)).alias("open_businesses"),
    count(when(col("is_open") == 0, True)).alias("closed_businesses")
)

display(business_stats)

# COMMAND ----------

# DBTITLE 1,Review Analysis Section
# MAGIC %md
# MAGIC ## Review Dataset Analysis

# COMMAND ----------

# DBTITLE 1,Review Schema and Sample
# Review dataset schema and sample
print("=== Review Dataset Schema ===")
review_df.printSchema()

print("\n=== Sample Records ===")
display(review_df.select("review_id", "business_id", "user_id", "stars", "date", "useful", "funny", "cool").limit(10))

# COMMAND ----------

# DBTITLE 1,Review Statistics
from pyspark.sql.functions import year, month, length

# Review statistics
review_stats = review_df.select(
    count("*").alias("total_reviews"),
    countDistinct("business_id").alias("reviewed_businesses"),
    countDistinct("user_id").alias("unique_reviewers"),
    avg("stars").alias("avg_rating"),
    avg("useful").alias("avg_useful"),
    avg("funny").alias("avg_funny"),
    avg("cool").alias("avg_cool"),
    avg(length("text")).alias("avg_review_length")
)

display(review_stats)

# COMMAND ----------

# DBTITLE 1,Review Rating Distribution
# Rating distribution in reviews
review_rating_dist = review_df.groupBy("stars") \
    .agg(count("*").alias("count")) \
    .orderBy("stars")

display(review_rating_dist)

# COMMAND ----------

# DBTITLE 1,Review Trends Over Time
# Reviews over time (by year)
review_trends = review_df.withColumn("year", year(col("date"))) \
    .groupBy("year") \
    .agg(count("*").alias("review_count"),
         avg("stars").alias("avg_rating")) \
    .orderBy("year")

display(review_trends)

# COMMAND ----------

# DBTITLE 1,User Analysis Section
# MAGIC %md
# MAGIC ## User Dataset Analysis

# COMMAND ----------

# DBTITLE 1,User Schema and Sample
# User dataset schema and sample
print("=== User Dataset Schema ===")
user_df.printSchema()

print("\n=== Sample Records ===")
display(user_df.select("user_id", "name", "review_count", "yelping_since", "fans", "average_stars").limit(10))

# COMMAND ----------

# DBTITLE 1,User Statistics
# User statistics
user_stats = user_df.select(
    count("*").alias("total_users"),
    avg("review_count").alias("avg_reviews_per_user"),
    max("review_count").alias("max_reviews"),
    avg("fans").alias("avg_fans"),
    max("fans").alias("max_fans"),
    avg("average_stars").alias("avg_user_rating"),
    avg("useful").alias("avg_useful_votes"),
    avg("funny").alias("avg_funny_votes"),
    avg("cool").alias("avg_cool_votes")
)

display(user_stats)

# COMMAND ----------

# DBTITLE 1,Most Active Users
# Top 20 most active users
top_users = user_df.select("name", "review_count", "fans", "average_stars", "useful") \
    .orderBy(col("review_count").desc()) \
    .limit(20)

display(top_users)

# COMMAND ----------

# DBTITLE 1,Tip Analysis Section
# MAGIC %md
# MAGIC ## Tip Dataset Analysis

# COMMAND ----------

# DBTITLE 1,Tip Schema and Sample
# Tip dataset schema and sample
print("=== Tip Dataset Schema ===")
tip_df.printSchema()

print("\n=== Sample Records ===")
display(tip_df.select("business_id", "user_id", "text", "date", "compliment_count").limit(10))

# COMMAND ----------

# DBTITLE 1,Tip Statistics
# Tip statistics
tip_stats = tip_df.select(
    count("*").alias("total_tips"),
    countDistinct("business_id").alias("businesses_with_tips"),
    countDistinct("user_id").alias("users_giving_tips"),
    avg("compliment_count").alias("avg_compliments"),
    avg(length("text")).alias("avg_tip_length")
)

display(tip_stats)

# COMMAND ----------

# DBTITLE 1,Checkin Analysis Section
# MAGIC %md
# MAGIC ## Checkin Dataset Analysis

# COMMAND ----------

# DBTITLE 1,Checkin Schema and Sample
# Checkin dataset schema and sample
print("=== Checkin Dataset Schema ===")
checkin_df.printSchema()

print("\n=== Sample Records ===")
display(checkin_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Checkin Statistics
from pyspark.sql.functions import size, split

# Checkin statistics
checkin_stats = checkin_df.select(
    count("*").alias("total_businesses_with_checkins"),
    avg(size(split(col("date"), ", "))).alias("avg_checkins_per_business"),
    max(size(split(col("date"), ", "))).alias("max_checkins")
)

display(checkin_stats)

# COMMAND ----------

# DBTITLE 1,Relationship Analysis Section
# MAGIC %md
# MAGIC ## Cross-Dataset Relationship Analysis

# COMMAND ----------

# DBTITLE 1,Business vs Actual Review Counts
# Join business with review counts
business_review_analysis = business_df.alias("b") \
    .join(review_df.groupBy("business_id").agg(count("*").alias("actual_review_count")),
          on="business_id", how="left") \
    .select(
        col("b.name"),
        col("b.city"),
        col("b.stars"),
        col("b.review_count").alias("stated_review_count"),
        col("actual_review_count")
    ) \
    .orderBy(col("actual_review_count").desc()) \
    .limit(20)

display(business_review_analysis)

# COMMAND ----------

# DBTITLE 1,User Engagement Summary Statistics
# User engagement correlation
user_engagement = user_df.select(
    "review_count",
    "fans",
    "useful",
    "funny",
    "cool",
    "average_stars"
).summary("count", "mean", "stddev", "min", "max")

display(user_engagement)