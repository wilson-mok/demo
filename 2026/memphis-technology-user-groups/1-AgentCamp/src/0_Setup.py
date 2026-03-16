# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE CATALOG IF NOT EXISTS demo_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS demo_catalog.demo_receipts;
# MAGIC CREATE VOLUME IF NOT EXISTS demo_catalog.demo_receipts.raw;
# MAGIC CREATE VOLUME IF NOT EXISTS demo_catalog.demo_receipts.raw_training;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Clean up
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_receipts.bronze_receipts;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_receipts.silver_receipts;
# MAGIC DROP TABLE IF EXISTS demo_catalog.demo_receipts.silver_receipts_info_extract;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Copy Recipts
# MAGIC 1. Download the receipt images from https://www.kaggle.com/datasets/dhiaznaidi/receiptdatasetssd300v2/data.
# MAGIC 1. Unzip the files and copy the receipts:
# MAGIC   - For training dataset: Copy 20 receipts to /Volumes/demo_catalog/demo_receipts/raw_training/
# MAGIC   - For processing: Copy any remaining receipts to /Volumes/demo_catalog/demo_receipts/raw/
# MAGIC