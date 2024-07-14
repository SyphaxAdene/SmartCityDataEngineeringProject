# Smart City Data Fusion Project

## Overview
The Smart City Data Fusion Project aims to integrate and analyze diverse datasets to gain insights into urban dynamics. Specifically, this project focuses on studying the impact and influence of events and weather on the availability of parking spots in a smart city environment.

## Purpose
The purpose of this project is to:
1. **Ingest and Integrate Data**: Collect data from various sources, including event schedules, weather forecasts, and parking availability.
2. **Transform and Clean Data**: Process the raw data to ensure it is clean, consistent, and ready for analysis.
3. **Validate Data**: Apply validation rules to ensure data accuracy and reliability.
4. **Analyze and Gain Insights**: Use the fused data to identify patterns and correlations between events, weather conditions, and parking availability.

## Directory Structure

- DBW_JOBS/
  - configs/
    - databricks/
      - workflows/
        - clean_env_workflow.json
        - ingestion_workflow.json
        - smart_city_workflow.json
  - dbw_env/
  - src/
    - cleaning/
      - cleaning_env.py
    - jobs/
      - bronze_to_silver.py
      - silver_to_gold.py
      - smart_city_data_fusion.py
    - validation/
      - rules/
        - events_data_validation_rules.yaml
        - parking_data_validation_rules.yaml
        - source_name_validation_rules-template.yaml
      - validator.py
  - test/
  - azure-pipelines.yaml
  - README.txt
  - requirements.txt
  - setup.py

## Components

### configs/databricks/workflows/
- **clean_env_workflow.json**: Defines the workflow for cleaning the environment.
- **ingestion_workflow.json**: Defines the workflow for data ingestion processes.
- **smart_city_workflow.json**: Defines the workflow for the overall smart city data fusion process.

### src/
Contains the main source code for the project.

#### cleaning/
- **cleaning_env.py**: Script for setting up the environment for data cleaning tasks.

#### jobs/
- **bronze_to_silver.py**: Script for transforming raw data (bronze) to a more refined state (silver).
- **silver_to_gold.py**: Script for further refining the silver data to a high-quality state (gold).
- **smart_city_data_fusion.py**: Main orchestration script for integrating different data sources and applying transformations.

#### validation/
- **rules/**:
  - `events_data_validation_rules.yaml`: Validation rules specific to event data.
  - `parking_data_validation_rules.yaml`: Validation rules specific to parking data.
  - `source_name_validation_rules-template.yaml`: Template for source name validation rules.
- **validator.py**: Contains validation logic for the ingested data.