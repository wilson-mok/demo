# Azure Databricks - Getting Started with Vector Search

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes:

* Prepared our Semi-structure Markdown data using Medallion Architecture.
* Using Mosaic AI Vector Search to create embedding from our Gold table.
* Best practices on using Vector Search Index.


After this session, you will be able utilize Databricks for your RAG workflow. 

## Presentation & Video
* Presentation: [Azure Databricks - Getting Started with Vector Search PDF](./Azure%20Databricks%20-%20Getting%20Started%20with%20Vector%20Search.pdf)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks with Unity Catalog enabled. 

## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| [DBC File](./src/2025%20-%20EDSC%20-%20DE%20with%20Vector%20Search.dbc) | This DBC file contains 4 scripts. It takes you the journey from setup, extracting VSCode documentation (MD Files), processing it through Medallion architecture, creating the RAG and executing similarity search on the data. |


| Data Folder/file | Description |
| --- | --- |
| The data file is retrieved using by running the 1 - Extract and Copy VSCode Doc notebook | The Markdown files are from https://github.com/microsoft/vscode-docs |