# Azure Databricks (Free Edition) - Trusted Data Assistant

The purpose of this article is to provide the setup instructions for the demo/presentation. The presentation and demo includes:
* Sign-up for a free Databricks account.
* Prepare the data for our Data Assistant.
* What is Genie and how it helps users get answers from data using natural languages?
* Refine Genie's repsonse to improve the response accuracy of the Data Assistant.


After this session, you will be able to utilize Databricks (Free Edition) to create and refine your own Data Assistant.

## Presentation & Video
* Presentation: [Azure Databricks - Trusted Data Assistant](./Azure%20Databricks%20-%20Trusted%20Data%20Assistant.pdf)

## Setup
### Pre-req
For this demo, please sign-up for the [Databricks Free Edition](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition). 


## Code and scripts
All the scripts can be found under the [src directory](src/).
| Src filename | Description |
| --- | --- |
| 1_Setup.py | This script setup the required catalog, schema, and volume. |
| 2_Bronze_Table.py | Prior running this script, please copy the Yelp JSON files to the raw_data Volume. This script create the Bronze Tables using the JSON data. |

| 3_Silver_Table.py | Extract key information out of Bronze Table. This data is ready for Genie - Data Assistant. |
| 4_Metadata_and_Relationships.sql | Create the Metadata required for the Silver tables. |
| 5_Genie_setup.md | Instructions for creating a refined Genie. |
| Genie_questions.txt | Questions used to for the Genie demo. |

| Data Folder/file | Description |
| --- | --- |
| [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/) | This file contains the Yelp data for the Genie demo. Download the JSON and upload the JSON files to the 'raw_data' managed volume. [Documentation](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-to-volume)|
