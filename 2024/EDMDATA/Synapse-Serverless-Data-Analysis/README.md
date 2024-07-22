# Azure Synapse Analytics - Data Analysis

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes:

* Overview of Azure Synapse Analytics
* What is Serverless SQL and Serverless Spark and the benefits of using it.
* Explore the importance of Data Analysis - Why accurate and relevant data are important?
* Demo: Serverless SQL and Serverless Spark to review, validate and create visualization using the sample data.

After this session, you will be able to use Azure Synapse Analytics to conduct your own data analysis for a better result.

## Video
Link: [Youtube - Conduct Data Analysis using Azure Synapse Analytics | Serverless SQL | Serverless Spark](https://youtu.be/-Jfb40xN7uU?list=PLd5EI5E5dBo5Pj2v10QN_orpbY7QBYQxF)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Synapse Analytics with a 'Small' spark pool.
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. A dedicated 'demo' container to be created in the Azure Storage. This container will store all the data files.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| [NY Taxi - Sample data](../../../sample/test-data/nycTripYellow2019Jan/) | Download all 4 NY Taxi sample files. Dataset Reference: [Kaggle](https://www.kaggle.com/datasets/microize/newyork-yellow-taxi-trip-data-2020-2019?select=yellow_tripdata_2019-01.csv) |
| [Demo 1 - Create Dataset - SQL](./src/d1_create_sql_dataset.sql) | This SQL script will create the Schema and External Table from the parquet file. |
| [Demo 1 - Analyze Dataset - SQL](./src/d1_analysis_sql_dataset.sql) | This SQL script illustrates the steps to conduct a data analysis using Serverless SQL. |
| [Demo 2 - Create & Analyze Dataset - PySpark](./src/d2_analysis_pyspark_dataset.ipynb) | This PySpark script will examine, clean the data and create a Table in the Lake DB. |
| [Demo 2 - Serverless SQL to access Lake DB](./src/d2_query_sql_lakedb.sql) | This SQL script illustrates the ability for Serverless SQL to query Lake Database. |