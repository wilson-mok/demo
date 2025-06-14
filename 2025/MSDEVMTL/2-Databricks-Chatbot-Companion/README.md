# Azure Databricks - Building a Chatbot Companion with AI/BI Genie

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo include:

– Introduction to AI/BI Genie.
– Customization techniques to tailor Genie's response to deliver consistent and trustworthy insights.
– Applying the best practices and techniques to create our own Genie.
- Develop a Streamlit app using Databricks' Genie API.

After this session, you will be able to customize and optimize your own data companion. 

## Presentation & Video
* Presentation: [Azure Databricks - Building a Chatbot Companion with AI/BI Genie](Azure%20Databricks%20-%20Building%20a%20Chatbot%20Companion%20with%20AIBI%20Genie.pdf)

## Setup
### Pre-req

For this demo, we require:

1. Azure Databricks with Unity Catalog enabled.
1. A Catalog named 'demo_catalog' has been created.
1. A Schema named 'demo_yelp_academic' has been created under 'demo_catalog'.
1. A Managed Volume named 'raw_data' has been created under the 'demo_catalog'.

## Code and scripts

Below contains the scripts and configuration details for the Genie. The creation of the Genie will have to be manual. Please follow along using the video recording.

Note: Yelp might provide data from your city instead of Edmonton. If any Genie configuration/test cases refer to Edmonton or Alberta, please substitute it with your location.

### Data for Genie

| Src filename | Description |
| --- | --- |
| [1-Create Table.py](./src/1-Create%20Table.py) | Pre-req: The Yelp JSON files uploaded are uploaded to the 'raw_data' managed volume. This script will read the business, checkin, review, tip, and user JSONs and create the bronze datasets. |
| [2-Transform Data.py](./src/2-Transform%20Data.py) | This script extracts and flattens the data from the JSON to create the Silver tables. |
| [3-Metadata and relationships.py](./src/3-Metadata%20and%20relationships.py) | This script will apply the metadata to the Silver tables. This includes table description, column comments and PK/FK. |

| Data Folder/file | Description |
| --- | --- |
| [Yelp Open Dataset](https://business.yelp.com/data/resources/open-dataset/) | This file contains the Yelp data for the Genie demo. Download the JSON and upload the JSON files to Databricks using [File Upload UI](https://learn.microsoft.com/en-us/azure/databricks/ingestion/file-upload/upload-data?wt.mc_id=MVP_365600) to the 'raw_data' managed volume. |

### Streamlit App
Will be avialable after the SQLSaturday

### Genie configuration
Will be available after the SQLSaturday