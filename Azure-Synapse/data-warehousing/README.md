# Azure Synapse - Data Warehousing demo
The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes: 
- What is Azure Synapse Analytics?
- What is Data Warehousing?
- How to design and create a Dimensional data model?
- A demo of using Azure Synapse Analytics to create a data pipeline to store data into the data warehouse.

In the end, you will be ready to create your own data warehousing solution in Azure Synapse Analytics.

![Azure Synapse - Data Warehousing diagram](./Azure%20Synapse%20-%20Data%20Warehousing.png)


## Video
Link: [Youtube - Dear Azure - Data Warehousing with Azure Synapses | Azure Synapse best practice | Dedicated SQL Pool](https://youtu.be/DOF4fEQSseQ)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Synapse Analytics with a Dedicated SQL pools (Size: DW100c)
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. A dedicated 'data' container to be created in the Azure Storage. This container will store all the data files.
1. An Azure SQL database to act as a source data.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| src/azureSql/data/ | This folder contains all the SQL files for the Azure SQL database. Please run CreateSchema.sql first then the rest of the table files can be ran in any order. |
| src/azureSql/queries.sql | This file contains the queries used to build better understand of the data in the Azure SQL database. |
| src/data-warehouse/createDWHScript.sql | This SQL script will create the schema and the dimensional data model in the Dedicated SQL Pool. |
| src/data-warehouse/queries.sql | This SQL script contains the queries to validate the result of the data pipeline. |
| src/synapse-pipeline/ | This folder contains all the Synapse JSON code. You need to make changes to the Linked services using your Azure resources. |

