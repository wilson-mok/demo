# Azure Databricks - Vector Search

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes:

* Simplifying the RAG pipelines using Databricks Vector Search.
* Using Unity Catalog's delta table and volume to store and manage our data.
* Best practices on using Vector Search Index.


After this session, you will be able utilize Databricks for your RAG workflow. 

## Presentation & Video
* Presentation: [Azure Databricks - Vector Search PDF](./Azure%20Databricks%20-%20Vector%20Search.pdf)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks with Unity Catalog enabled. 

## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| 0-bu_ops_project_blog_insight_setup.py | This script setup the catalog, schema and Vector Search endpoint for our demo. |
| 1-blog_ingest_and_processing.py | This script saves the blog's Markdown files from github. It extracts its metadata and content and save it as a delta file. |
| 2-blog_vector_search.py | This script creates the Vector Search index and the writeback table. | 

| Data Folder/file | Description |
| --- | --- |
| [Genie - Sample Customers data](../../../demo-data/test-data/sample_customers_data.csv) | This file contains the Customers data for the Genie demo. Download this file and upload to Databricks using [File Upload UI](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-data?wt.mc_id=MVP_365600). |
| [Genie - Sample Invoices data](../../../demo-data/test-data/sample_invoices_data.csv) | This file contains the Invoices data for the Genie demo. Download this file and upload it to Databricks. |
| [Genie - Sample Invoice Linesdata](../../../demo-data/test-data/sample_invoices_data.csv) | This file contains the Invoice Lines data for the Genie demo. Download this file and upload it to Databricks. |