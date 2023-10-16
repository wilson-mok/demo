# Azure Databricks - Building streaming pipeline
The purpose of this README file is to provide the setup instructions for the recorded demo on

   * Design and implement streaming pipelines using Azure Databricks and Azure Data Factory.
   * A quick introduction to Delta Live Table in Azure Databricks.

After this session, you will be able to create your own streaming data pipeline for your Lakehouse using Azure Databricks and Azure Data Factory.

## Video
Link: [Youtube - Dear Azure - Azure Databricks | Azure Data Factory | Building a Streaming Data Pipeline](https://youtu.be/R8Sd2_75Fi8) 

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks - Premium edition
1. Azure Data Factory
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. Create the 'landing', 'bronze', 'silver' and 'gold' containers in the Storage account.
     - The landing container will store all the data files. Please copy the folder structure from data\data-lake\landing\.
     - The Bronze, Silver and Gold containers store the delta files. 

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/adf | This folder contains all the ADF code. You need to make changes to the Linked services using your Azure resources. | 
| src/databricks/iotPowerConsumption_demo4.dbc | This file contains all the pyspark scripts. You need to import this file into Databricks. |
| src/databricks/dlt_pipeline.json | This file contains Delta Live table pipeline. You can copy and paste the JSON content into the your pipeline in Databricks. |
| data/data-lake/landing | This folder contains the source data files used in the demo. This data is created on for Jan 1, 2023 and sent to the Data Lake based on 15 min interval. |
