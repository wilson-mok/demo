# Azure Data Factory - CI/CD demo
The purpose of this article is to provide the setup instructions for the recorded demo. The presentation and demo includes: 
- Setting up a git repository in Azure Data Factory.
- What is the continuous integration and delivery (CI/CD) process?
- A demo of CI/CD release process in Azure DevOps:
     - Creating and deploying the CI/CD pipeline.
     - Configure environment specific settings.  

At the end of this session, you will be ready to deploy your data pipelines from development to production.

## Video
Link: [Youtube - Dear Azure - CI/CD with Azure Data Factory | ADF Deployment](https://youtu.be/r5sxL99UrJk)

## Setup
### Pre-req
In this demo, we will be using Azure Devops pipeline to impleement the CI/CD process for Azure Data Factory.
1. We will need 2 environments: Development and Production. Each environment requires an Azure Data Factory, Azure SQL Database, Azure Key Vault and Data Lake gen 2.
1. Azure DevOps project and a repo are required.
1. Service connection: A Service Principle with a Contirbutor access to the Azure Subscription is required for Azure DevOps to connect to Azure Subscription.
1. The Dev - Azure Data Factory requires to have data pipeline deployed. 

### Integration
![Service integration](./images/ADF-cicd.png)  
The detail integration between the different Azure services, please refer to the [Data pipeline README.md](../data-pipeline/README.md).

## Code and scripts
| Folder/file | Description |
| --- | --- |
| [Data pipeline](../data-pipeline) | Please refer to the Azure Data Factory Data pipeline demo for the inital setup. In this demo, two seperate environments are required. We need to deploy a the same Azure services for Development and Produciton. |
| build/package.json | This JSON file contains the location of the Azure Data Factory Utilities and its dependency. |
| build/azure-adf-build-deploy-dev-pipeline.yml | This file contains the build (CI) and deployment to the (Dev) Azure Data Factory - Live mode (CD). |
| build/azure-adf-deploy-prd-pipeline.yml | This file contains the build (CI) and deployment to the (Prd) Azure Data Factory - Live mode (CD). |

