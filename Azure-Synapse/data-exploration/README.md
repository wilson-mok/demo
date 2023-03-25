# Azure Synapse - Data Exploration demo
The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes: 
- What is Azure Synapse Analytics?
- How to conduct a Data Exploration?
- A demo of using Azure Synapses prepare, clean and analyze data for data exploration
     - Using Serverless SQL to create external tables and build queries for data analysis.
     - Using Serverless Spark to analyze the data and produce a 'clean data' for Serverless SQL to consume.

In the end, you will be ready to conduct your own data exploration in Azure Synapse Analytics.

## Video
Link: [Youtube - Dear Azure - Data Exploration with Azure Synapses | Azure Synapse best practice](https://youtu.be/-OUDrRLJ3gY)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Synapse Analytics with a 'Small' spark pool.
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. A dedicated 'data' container to be created in the Azure Storage. This container will store all the data files.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/p_download_opendata_nycTaxiYellow2019.ipynb | This PySpark file contains the code to extract the Jan 2019 - NYC Trip Yellow Tax data from [Azure Open Dataset](https://docs.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow?tabs=pyspark#azure-synapse). |
| requirements.txt | This script contains the library used in the demo. This needs to be install in the Spark cluster: You can follow the instruction [here](https://docs.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-python-packages). |
| src/d1_create_sql_nycTripYellow2019Jan.sql | This SQL script will create the Schema and External Table from the parquet file. |
| src/d1_explore_sql_nycTripYellow2019Jan.sql | This SQL script illustrates the steps to conduct a data exploration in Serverless SQL. |
| src/d2_exploration_pyspark_myTripYl2019Jan.ipynb | This PySpark script will examine, clean the data and create a Table in the Lake DB. |
| src/d2_sparkTableQuery.sql | This SQL script illustrates the ability to query from both SQL DB and Lake DB. |
