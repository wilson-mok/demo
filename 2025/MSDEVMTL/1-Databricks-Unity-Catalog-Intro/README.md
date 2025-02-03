# Azure Databricks - Unity Catalog Intro

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes:

* Understand the core concepts and high-level architecture of the Unity Catalog.
* Implement fine-grained access control and track data lineage.
* Best Practices for designing and organizing our first catalog.

After this session, you will be able to improve your data governance using Azure Databricks.  

## Presentation & Video
* Presentation: [Azure Databricks - Intro to Unity Catalog PDF](./Azure%20Databricks%20-%20Intro%20to%20UC.pdf)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Databricks with Unity Catalog enabled. Two instances: 1. Admin, 1 - Enterprise Data, 2 - BU - Operations. 
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2) as your external location for delta lake.
1. Grant Admin the Unity Catalog Access Connector access to the delta lake Storage account (Storage Blob Data Contributor).
1. In the Admin workspace, setup the required Storage Credentials using the Admin's Access Connector.

## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| 1_admin_setup_ent_dev.py | This script is to be ran in the Admin workspace. It will setup the 'ent_data' catalog and the required schemas underneath. Specific permissions are applied to allow BU - Operations workspace access to Silver and Gold data (read-only).
| 2_bronze_*.py | Execute the scripts in the Enterprise Data workspace. This will retrieve the landing data from Volume and create the bronze layer of the delta lake (Medallion Architecture). |
| 3_silver_*.py | Execute the scripts after the bronze layer is created successfully. This will create the silver layer of the delta lake (Medallion Architecture). |
| 4_gold_*.py | Execute the scripts after the silver layer is created successfully. This will create the gold layer of the delta lake (Medallion Architecture). |
| 5__admin_setup_bu_ops.py | This script will deploy catalog for Operations team to use. Run this script in the Admin workspace. |
| 5_bu_ops_projectNextus_setup.py | This script uses the data from the gold layer (Ent Data) and combine with data from the Project Nexus. Some of the steps require you to 'Create or modify table' using the Databricks UI. This will create a Managed Table. Another step will require you to 'Upload files to a volume' using the Databricks UI to utilize the Managed Volume. |
| 6_admin_audit_logs.py | This scripts uses the `system.access.audit` table (preview) to compliance reporting. This script is to be ran in the Admin workspace. |

| Data Folder/file | Description |
| --- | --- |
| [Data - Landing](../../../demo-data/iot-smart-grid-data/landing/) | Download this files in the landing folder to upload it to your landing container in your external Storage account. |
| [Data - Project Nexus](../../../demo-data/iot-smart-grid-data/projectnexus/) | Download this files for the Project nexus demo. |