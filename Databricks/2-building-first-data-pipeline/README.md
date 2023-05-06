# Azure Databricks - Building your first data pipeline
The purpose of this README file is to provide the setup instructions for the recorded demo on

   * Creating a Databricks notebook to process data using delta lake format.
   * Produce an external table for Dashboard consumption in Databricks.
   * Schedule the notebook to run automatically using Azure Data Factory.

After this session, you will be able to create your own data pipeline in Azure Databricks and create a schedule for it to run automatically with Azure Data Factory.

## Video
Link: [Youtube - Dear Azure - Azure Databricks | Azure Data Factory | Building your first data pipeline](https://youtu.be/YDGA67NgE1s) 

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks - Premium edition
1. Azure Data Factory
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. Create the 'landing' and 'processed' containers in the Storage account.
   a. The landing container will store all the source data files (JSON format).
   a. The processed container will store the output data stored in delta lake format.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/adf | This folder contains all the ADF code. You need to make changes to the Linked services using your Azure resources. | 
| src/databricks/iotPowerConsumption_demo2-1.ipynb | This file contains the code to read the power consumption data in the data lake and create the output file in the 'processed' container using delta lake format. |
| src/databricks/iotPowerConsumption_demo2-2.dbc | This file contains the SQL and dashboard for the power consumption data. You need to import this file into Databricks. |
| data/zipCodeLongLat.csv | This data file contains the conversion between US Zip Code and map coordinates (Longitude and Latitude). Upload this file directly into Databricks' Data tab. |
| data/data-lake/landing | This folder contains the source power consumption JSON files used in the demo. This data is created on for Jan 1, 2023 and sent to the Data Lake based on 15 min interval. |
