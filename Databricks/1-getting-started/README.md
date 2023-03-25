# Azure Databricks - Getting started
The purpose of this README file is to provide the setup instructions for the recorded demo. The presentation and demo includes: 
-	Introduce Databricks and its features.
-	Compare the Databricks Community editions and Azure Databricks Premium editions.
-	Provide a brief tour of the Databricks UI. 
-	A demo of using Databricks to query data using PySpark.

In the end, you can use Databricks to create a dashboard in Databricks. 

## Video
Link: [Youtube - Dear Azure - Azure Databricks | Getting Started](https://youtu.be/WWnSd7ydC78)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks - Premium edition
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. A 'landing' container created in the Azure Storage. This container will store all the power consumption data files.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/iotPowerConsumption_demo2.ipynb | This file contains the code to read the data in the data lake and create a dashbarod in Databricks. You can import this notebook into your Databricks workspace. |
| data/zipCodeLongLat.csv | This data file contains the conversion between US Zip Code and map coordinates (Longitude and Latitude). |
| data/data-lake/ | This folder contains all the power consumption files used in the demo. |
