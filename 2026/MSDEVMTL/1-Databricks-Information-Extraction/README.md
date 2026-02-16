# Azure Databricks - Unlocking Unstructured Data for Analytics

The purpose of this article is to provide the setup instructions for the demo/presentation. The presentation and demo includes:
* The challenges of working with unstructured data and how GenAI changed the landscape.
* How Databricks AI-driven document parser and Agent Brick Information Extraction addresses those challenges?
* How does unstructured data fits into Medallion Architecture?


After this session, you will be able to utilize Azure Databricks to develop workflows to extract valuable content from Unstructured Data.

## Presentation & Video
* Presentation: [Azure Databricks - Unlocking Unstructured Data for Analytics PDF](Azure%20Databricks%20-%20Unlocking%20Unstructured%20Data%20for%20Analytics.pdf)

## Setup
### Pre-req
For this demo, the Azure Databricks workspace has to be setup based on [documented requirements](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-bricks/key-info-extraction#requirements).

Note: at the time of this presentation Agent Bricks - Information Extraction and ai_parse_document function are in *preview*. There is limitated region those services are available.

## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| 0_Setup.py | This script setup the required catalog, schema, and volume. |
| 1_Data Ingestion.py | Prior running this script, please copy the receipt into the raw and raw_training Volume. This script create the Bronze Table for the receipt data based on the data in the Volume. |
| 2-1_Transformation.py | Execute this script to create the Silver table. This table is created using ai_parse_document and extracted text from the receipt images. |
| 2-2_Transformation_info_extract.py | Prior executing this script, please create the Information Extraction agent and obtain the serving endpoint. Execute this script to create the Silver 'Info Extract\ table using the agent endpoint. |
| 3_Cost_Query.sql | Execute this script to help you understand the cost of running ai_parse_document and Information Extraction agent. |


| Data Folder/file | Description |
| --- | --- |
| [Receipt data on Kaggle](https://www.kaggle.com/datasets/dhiaznaidi/receiptdatasetssd300v2) | Download the image files and copy them into the raw and raw_training Volumes. |