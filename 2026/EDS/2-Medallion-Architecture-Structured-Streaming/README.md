# Azure Databricks (Free Edition) - Medallion Architecture w/ Structured Streaming

The purpose of this article is to provide the setup instructions for the demo/presentation. The presentation and demo includes:
* Sign-up for a free Databricks account.
* Introduction to Medallion Architecture and why it is used?
* Understanding of batch vs streaming data pipeline. 
* Live Demo: Build a Medallion Architecture with Structured Streaming using Yelp Open Dataset.


After this session, you will be able to utilize Databricks (Free Edition) to create your own data pipelines

## Presentation & Video
* Presentation: Azure Databricks - Medallion Architecture with Structured Streaming

## Setup
### Pre-req
For this demo, please sign-up for the [Databricks Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition). 


## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| 1_Setup.py | This script setup the required catalog, schema, and volume. |
| TBD |  |


| Data Folder/file | Description |
| --- | --- |
| [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/) | This file contains the Yelp data for the Genie demo. Download the JSON and upload the JSON files to the 'raw_data' managed volume. [Documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-to-volume)|
