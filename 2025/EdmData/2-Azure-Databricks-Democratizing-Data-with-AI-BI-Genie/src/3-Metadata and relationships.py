# Databricks notebook source
# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC -- PRIMARY KEYS
# MAGIC ALTER TABLE silver_user ALTER COLUMN user_id SET NOT NULL;
# MAGIC ALTER TABLE silver_user ADD CONSTRAINT pk_silver_user PRIMARY KEY (user_id);
# MAGIC
# MAGIC ALTER TABLE silver_business ALTER COLUMN business_id SET NOT NULL;
# MAGIC ALTER TABLE silver_business ADD CONSTRAINT pk_silver_business PRIMARY KEY (business_id);
# MAGIC
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_id SET NOT NULL;
# MAGIC ALTER TABLE silver_review ADD CONSTRAINT pk_silver_review PRIMARY KEY (review_id);
# MAGIC
# MAGIC -- FOREIGN KEYS
# MAGIC ALTER TABLE silver_review ADD CONSTRAINT fk_review_user FOREIGN KEY (user_id) REFERENCES silver_user(user_id);
# MAGIC ALTER TABLE silver_review ADD CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES silver_business(business_id);
# MAGIC
# MAGIC ALTER TABLE silver_tip ADD CONSTRAINT fk_tip_user FOREIGN KEY (user_id) REFERENCES silver_user(user_id);
# MAGIC
# MAGIC ALTER TABLE silver_tip ADD CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES silver_business(business_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC -- FOREIGN KEYS
# MAGIC ALTER TABLE silver_review ADD CONSTRAINT fk_review_user FOREIGN KEY (user_id) REFERENCES silver_user(user_id);
# MAGIC ALTER TABLE silver_review ADD CONSTRAINT fk_review_business FOREIGN KEY (business_id) REFERENCES silver_business(business_id);
# MAGIC
# MAGIC ALTER TABLE silver_tip ADD CONSTRAINT fk_tip_user FOREIGN KEY (user_id) REFERENCES silver_user(user_id);
# MAGIC ALTER TABLE silver_tip ADD CONSTRAINT fk_tip_business FOREIGN KEY (business_id) REFERENCES silver_business(business_id);

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC ALTER TABLE silver_business
# MAGIC SET TBLPROPERTIES (
# MAGIC   'comment' = "The yelp business \'Silver\' table contains essential information about various businesses on Yelp, including their names, locations, ratings, and review counts. It also includes details such as categories, price range, and amenities like wheelchair accessibility, drive-thru, takeout, and free Wi-Fi. This table serves as a comprehensive repository of business data that can be used for analysis, decision-making, and customer insights within the Yelp academic dataset."
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE silver_business ALTER COLUMN business_id COMMENT 'Unique business identifier from Yelp.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN name COMMENT 'Name of the business.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN city COMMENT 'City where the business is located.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN state COMMENT 'State where the business is located.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN latitude COMMENT 'Latitude coordinate of the business.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN longitude COMMENT 'Longitude coordinate of the business.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN stars COMMENT 'Average star rating (1 to 5) from all user reviews.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN review_count COMMENT 'Total number of user reviews received.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN categories COMMENT 'List of business categories, parsed into an array.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN priceRange COMMENT 'Price tier of the business (1=budget friendly to 4=most expansive).';
# MAGIC ALTER TABLE silver_business ALTER COLUMN wheelchairAccessible COMMENT 'Indicates if the business is wheelchair accessible.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN driveThru COMMENT 'Indicates if the business has a drive-thru.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN takeOut COMMENT 'Indicates if the business offers takeout.';
# MAGIC ALTER TABLE silver_business ALTER COLUMN freeWifi COMMENT 'Indicates if the business offers free Wi-Fi.';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC ALTER TABLE silver_user
# MAGIC SET TBLPROPERTIES (
# MAGIC   'comment' = 'The yeip user \'Silver\' table contains data related to users on a business review platform. It includes information such as user ID, name, member since date, review count, review votes (cool, useful, funny), average stars given in reviews, and elite status details. This table is crucial for analyzing user engagement, reviewing trends, and identifying elite users based on their activity and reviews. The data in this table helps businesses understand user behavior, preferences, and loyalty over time.'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE silver_user ALTER COLUMN user_id COMMENT 'Unique identifier for the user.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN name COMMENT 'Display name of the Yelp user.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN member_since COMMENT 'Date when the user joined Yelp.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN review_count COMMENT 'Total number of reviews written by the user.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN review_cool_votes COMMENT 'Count of times the user’s reviews were marked as “cool.”';
# MAGIC ALTER TABLE silver_user ALTER COLUMN review_useful_votes COMMENT 'Count of times the user’s reviews were marked as “useful.”';
# MAGIC ALTER TABLE silver_user ALTER COLUMN review_funny_votes COMMENT 'Count of times the user’s reviews were marked as “funny.”';
# MAGIC ALTER TABLE silver_user ALTER COLUMN average_stars COMMENT 'Average star rating given by the user across all reviews.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN elite_years COMMENT 'Array of years when the user held elite status.';
# MAGIC ALTER TABLE silver_user ALTER COLUMN elite_year_count COMMENT 'Total number of years the user was part of the elite program.';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC ALTER TABLE silver_review
# MAGIC SET TBLPROPERTIES (
# MAGIC   'comment' = 'The yelp review \'Silver\' table contains data related to user reviews for businesses. It includes information such as review ID, user ID, business ID, review date, star rating, and various types of votes received on the review. The table serves as a repository for feedback provided by users on their experiences with different businesses. This data can be analyzed to understand customer sentiment, identify popular businesses, and improve overall customer satisfaction.'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_id COMMENT 'Unique identifier for the review.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN user_id COMMENT 'ID of the user who wrote the review.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN business_id COMMENT 'ID of the business being reviewed.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_date COMMENT 'Date when the review was submitted.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN stars COMMENT 'Star rating given by the user (1 to 5).';
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_useful_votes COMMENT 'Number of times other users marked the review as useful.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_funny_votes COMMENT 'Number of times other users marked the review as funny.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN review_cool_votes COMMENT 'Number of times other users marked the review as cool.';
# MAGIC ALTER TABLE silver_review ALTER COLUMN text COMMENT 'Full text content of the user’s review.';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE demo_catalog.demo_yelp_academic;
# MAGIC
# MAGIC ALTER TABLE silver_tip
# MAGIC SET TBLPROPERTIES (
# MAGIC   'comment' = 'The yelp tip \'Silver\' table contains data related to tips left by users on businesses. It includes information such as the user ID, business ID, tip date, text of the tip, compliment count, and whether the tip is considered popular. This table is valuable for analyzing user engagement with businesses, identifying popular tips, and understanding user sentiment towards different establishments.'
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE silver_tip ALTER COLUMN user_id COMMENT 'ID of the user who left the tip.';
# MAGIC ALTER TABLE silver_tip ALTER COLUMN business_id COMMENT 'ID of the business the tip refers to.';
# MAGIC ALTER TABLE silver_tip ALTER COLUMN tip_date COMMENT 'Date when the tip was posted.';
# MAGIC ALTER TABLE silver_tip ALTER COLUMN text COMMENT 'Short, informal message or advice provided by the user about the business.';
# MAGIC ALTER TABLE silver_tip ALTER COLUMN compliment_count COMMENT 'Number of compliments or likes received by the tip from other users.';
# MAGIC ALTER TABLE silver_tip ALTER COLUMN is_popular_tip COMMENT 'Boolean flag indicating if the tip is considered popular. (if compliment count > 3)';
# MAGIC

# COMMAND ----------

