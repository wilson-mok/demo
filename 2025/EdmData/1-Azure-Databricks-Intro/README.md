# Azure Databricks - Getting Started

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo include:

– Introduction to Azure Databricks
– Core Technologies Overview
– Creating well-annotated metadata for your data assets
– Explore Your Data using AI/BI Genie

After this session, you will be able to describe what Azure Databricks can do for your analytics needs, understand how different teams can work together in one platform, and even try out natural language querying with built-in AI tools like Genie.

## Presentation & Video

* Presentation: [Azure Databricks - Getting Started PDF](./Azure%20Databricks%20-%20Getting%20Started.pdf)
* Recording: https://youtu.be/cQFIxXQV3aU

## Setup

### Pre-req

For this demo, we require:

1. Azure Databricks with Unity Catalog enabled.

## Code and scripts

Databricks does not have the ability to export Genie, so the code will not be available. Please watch the video recording to create your own Genie. 

| Src filename | Description |
| --- | --- |
| [avg_profit_q1_2024.sql](./src/avg_profit_q1_2024.sql) | The SQL script to analysis the average profit in Q1 2024. |
| [profit_by_month_q1_20224.sql](./src/profit_by_month_q1_2024.sql) | The SQL script to analysis the profit by month in Q1 2024. For the visualization, use the video recording as a follow-along. |

| Data Folder/file | Description |
| --- | --- |
| [Genie - Sample Customers data](../../../demo-data/test-data/sample_customers_data.csv) | This file contains the Customers data for the Genie demo. Download this file and upload to Databricks using [File Upload UI](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-data?wt.mc_id=MVP_365600). |
| [Genie - Sample Invoices data](../../../demo-data/test-data/sample_invoices_data.csv) | This file contains the Invoices data for the Genie demo. Download this file and upload it to Databricks. |
| [Genie - Sample Invoice Lines data](../../../demo-data/test-data/sample_invoices_data.csv) | This file contains the Invoice Lines data for the Genie demo. Download this file and upload it to Databricks. |
