# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS demo_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_yelp_academic;
# MAGIC CREATE VOLUME IF NOT EXISTS demo_catalog.demo_yelp_academic.raw_data;
# MAGIC
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_business;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_checkin;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_review;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_tip;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_user;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_business;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_review;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_tip;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_user;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_business;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_checkin;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_review;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_tip;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.bronze_user;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_business;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_review;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_tip;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_yelp_academic.silver_user;

# COMMAND ----------

# Download the Yelp - Open Datset (JSON)
# https://business.yelp.com/data/resources/open-dataset/
# 
# Unzip the file and upload it to demo_catalog > demo_yelp_academic > raw_data