# Azure Databricks - Building a streaming Delta Lakehouse

The purpose of this README file is to provide the setup instructions for the recorded demo:

* Creating a Medallion delta lake
* Design and demo structure streaming for batch processing using Azure Data Factory and Azure Databricks.
* Using Databricks Serverless SQL to serve data to PowerBI.

After this session, you will be able to create your own streaming Delta Lakehouse using Azure Databricks and Azure Data Factory.

## Video

Link: TBD

## Setup

### Pre-req

For this demo, we require:

1. Azure Databricks - Premium edition
1. Azure Data Factory
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. Create the 'landing', 'bronze', 'silver' and 'gold' containers in the Storage account.
   a. The landing container will store all the data files. Please copy the folder structure from data\data-lake\landing\.
   a. The Bronze, Silver and Gold containers store the delta files.
1. Configure Azure Databricks with Unity Catalog enabled: [link] (https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/get-started)
1. Connect the Azure Stroage as an External Storage: [link] (https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-external-locations-and-credentials)
1. Create a group in Databricks named 'demo_group'. Add your user id to this group.

## Code and scripts

| Folder/file | Description |
| --- | --- |
| src/adf | This folder contains all the ADF code. You need to make changes to the Linked services using your Azure resources. |
| src/databricks/MSDEVMTL_Demo_1.dbc | This file contains all the pyspark scripts. You need to import this file into Databricks. Please start with demo1-00_cleanUp pyspark file. In each pyspark file, please update the dataLakeName widget/parameter.|
| data/data-lake/landing | This folder contains the source data files used in the demo. |
