# Azure Databricks - Building your Lakehouse
The purpose of this README file is to provide the setup instructions for the recorded demo on

   * Create a Lakehouse using Medallion architecture.
   * Design a shared data model in the gold layer for multiple data projects to consume.
   * Implement the Azure Data Factory pipeline to automate orchestrate the Databricks notebooks.
   * Use Databricks SQL to connect to Power BI for reporting.

After this session, you will be able to create your own Lakehouse using Azure Databricks and Azure Data Factory.

## Video
Link: [Youtube - Dear Azure - Azure Databricks | Azure Data Factory | Building a Lakehouse - Medallion Architecture](https://youtu.be/_zKHLjDfXes) 

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks - Premium edition
1. Azure Data Factory
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. Create the 'landing', 'bronze', 'silver' and 'gold' containers in the Storage account.
   a. The landing container will store all the data files. Please copy the folder structure from data\data-lake\landing\.
   a. The Bronze, Silver and Gold containers store the delta files. 

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/adf | This folder contains all the ADF code. You need to make changes to the Linked services using your Azure resources. | 
| src/databricks/iotPowerConsumption_demo2-2.dbc | This file contains all the pyspark scripts. You need to import this file into Databricks. |
| data/data-lake/landing | This folder contains the source data files used in the demo. This data is created on for Jan 1, 2023 and sent to the Data Lake based on 15 min interval. |
