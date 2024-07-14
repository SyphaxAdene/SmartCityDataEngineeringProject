# Azure Data Factory Pipelines

This repository contains a collection of Azure Data Factory (ADF) pipelines designed to ingest, process, and manage various data sources. The pipelines are organized to handle specific tasks such as copying data from SFTP to ADLS, running Databricks workflows, and integrating smart city data. Below is an overview of the project structure and the functionality of each pipeline.

## Project Structure

- **dataset**
  - `events_data_destination_dataset.json`
  - `events_data_source_dataset.json`

- **factory**
  - `syphax-data-factory.json`

- **integrationRuntime**
  - `integrationRuntime1.json`
  - `syphaxintegrationRuntime.json`

- **linkedService**
  - `ls_adf_db.json`
  - `ls_adls.json`
  - `ls_Az_Kv.json`
  - `ls_sftp.json`

- **pipeline**
  - **events_data**
    - `events_data_ingestion_main_pipeline.json`
  - **parking_data**
    - `parking_data_ingestion_main_pipeline.json`
  - **utils**
    - `copy_sftp_to_adls.json`
    - `run_databricks_workflow.json`
    - `cleaning_env.json`
  - `main_pipeline.json`
  - `run_cleaning_databricks_workflow.json`
  - `run_smart_city_databricks_workflow.json`
  - `smart_city_fusion_pipeline.json`
  - `publish_config.json`
  - `README.md`

## Pipelines Overview

### 1. Main Pipelines

#### `main_pipeline.json`
This is the primary pipeline that orchestrates the data ingestion and processing tasks. It consists of several sub-pipelines:
- **events_data_ingestion**
- **parking_data_ingestion**
- **smart_city_fusion**

### 2. Events Data Pipelines

#### `events_data_ingestion_main_pipeline.json`
This pipeline is responsible for ingesting events data. It consists of:
- **Execute Copy SFTP to ADLS**: Copies data from SFTP to ADLS.
- **Execute Run Databricks Job**: Runs a Databricks job to process the ingested data.

### 3. Parking Data Pipelines

#### `parking_data_ingestion_main_pipeline.json`
This pipeline handles the ingestion of parking data. It includes:
- **Execute Jobs API**: Runs a Databricks job for parking data processing.
- **Wait Until Job Completes**: Waits for the Databricks job to complete.
- **Job State Check**: Validates the job state and handles failures.

### 4. Utilities Pipelines

#### `copy_sftp_to_adls.json`
This utility pipeline copies files from an SFTP server to Azure Data Lake Storage (ADLS). It includes:
- **Get SFTP File List**: Retrieves the list of files from SFTP.
- **ForEach File**: Iterates over each file and copies it to ADLS.
- **Delete Source File**: Deletes the source file after successful copy.

#### `run_databricks_workflow.json`
This pipeline runs a Databricks job. It includes:
- **Execute Jobs API**: Triggers the Databricks job.
- **Wait Until Job Completes**: Waits for the job to finish and checks its status.
- **Check Final Job State**: Ensures the job completed successfully or handles errors.

### 5. Smart City Pipelines

#### `smart_city_fusion_pipeline.json`
This pipeline integrates various smart city data sources. It includes:
- **smart city**: Runs the `run_smart_city_databricks_workflow` pipeline to process smart city data.

### 6. Cleaning Pipelines

#### `cleaning_env.json`
This pipeline prepares the environment for data cleaning. It references:
- **run_cleaning_databricks_workflow**: Executes the data cleaning job on Databricks.

### 7. Configuration Files

- **integrationRuntime**: Contains configurations for integration runtimes.
- **linkedService**: Defines linked services for connecting to various data sources and sinks.
- **factory**: Configurations for the data factory.

## Getting Started

### Prerequisites
- Azure Subscription
- Azure Data Factory
- Azure Databricks
- Azure Blob Storage or Azure Data Lake Storage
- Integration with Azure Key Vault for storing secrets

### Deployment
1. Clone the repository.
2. Set up the linked services in Azure Data Factory.
3. Deploy the pipelines to Azure Data Factory.
4. Configure the required parameters and secrets in Azure Key Vault.
5. Trigger the `main_pipeline` to start the data ingestion and processing workflow.

### Contribution
Feel free to submit issues or pull requests for any improvements or bug fixes.
