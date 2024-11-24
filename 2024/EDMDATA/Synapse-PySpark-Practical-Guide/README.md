# Azure Synapse Analytics - A Practical Guide to PySpark

The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes:

* Explore Spark’s architecture and how it handles large, complex datasets.
* Dive the common challenges such as partitioning, data skew, etc.
* Demo: Commonly used operations and explore Spark UI to assist in troubleshooting.

After this session, you will be able to use PySpark for your data projects.

## Video
Link: [Practical Guide to Synapse Spark in Azure Synapse Analytics](https://youtu.be/e17DIBjlUmY?si=I3FjO0LAi0wfo5Ik)

## Setup
### Pre-req
For this demo, we require: 
1. Azure Synapse Analytics with a 'Small' spark pool.
1. Azure Storage with hierarchical namespace enabled (Data Lake gen 2)
1. A dedicated 'demo' container to be created in the Azure Storage. This container will store all the data files.

## Code and scripts
| Folder/file | Description |
| --- | --- |
| [Data - NYC Taxi Payment Type](../../../sample/test-data/nycTripYellowPaymentType/nycTripYellowPaymentType.csv) | Download this csv files and store it in the 'nycTripYellowPaymentType' folder under the 'demo' container. |
| [Demo 3-1 - Extract NYC Yellow Taxi data](./src/d3-1_Extract%20NYC%20Yellow%20Taxi.ipynb) | This PySpark script will extract a copy of the NYC Yellow Taxi data from Azure Open Data Storage |
| [Demo 3-2 - Practical Guide to PySpark](./src/d3-2_Practical%20Guide%20to%20PySpark.ipynb) | In this PySpark file, we will cover both Basic and Complex operations. Learn about Joins, Windows function, Partitioning and how to use Spark UI to help identify bottlenecks. |
