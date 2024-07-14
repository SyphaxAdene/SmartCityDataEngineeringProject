# Metadata Management for Data Engineering Project

This repository contains metadata definitions and management tools for our data engineering project focused on parking data analysis. It includes schema definitions, data lineage information, and pipeline configurations for processing parking and event data.

## Repository Structure

- `sources/events_data/events_data.yaml`: Metadata definition for events data processing
- `sources/parking_data/parking_data.yaml`: Metadata definition for parking station data processing
- `azure-pipelines.yaml`: Azure DevOps pipeline for uploading metadata to Databricks File System (DBFS)

## Metadata Files

### events_data.yaml

This file contains the schema and processing information for event data, including:

- Table name and description
- Data paths for bronze, silver, and gold layers
- Source information
- Key columns for each layer
- Detailed schema definitions for bronze, silver, and gold layers
- Partitioning strategy

### parking_data.yaml

This file contains the schema and processing information for parking station data, including:

- Table name and description
- Data paths for silver and gold layers
- Source information (Kafka configuration)
- Key columns for each layer
- Detailed schema definitions for bronze, silver, and gold layers
- Partitioning strategy

## Azure Pipeline

The `azure-pipelines.yaml` file defines an Azure DevOps pipeline that uploads the metadata files to a specified location in the Databricks File System (DBFS). This ensures that the latest metadata definitions are available for data processing jobs running on Databricks.

### Pipeline Configuration

- Triggered manually (no automatic triggers)
- Uses Ubuntu latest as the build agent
- Requires a variable group named 'databricks' containing the Databricks access token
- Defines variables for Databricks host and target DBFS path
- Installs the Databricks CLI and uploads metadata files to DBFS

## Usage

1. Update metadata files (`events_data.yaml` and `parking_data.yaml`) as needed when schema or processing logic changes.
2. Commit and push changes to this repository.
3. Run the Azure pipeline to upload the latest metadata to DBFS.
4. Data processing jobs on Databricks should reference these metadata files to ensure consistent schema and processing across the data pipeline.

## Best Practices

- Keep metadata files up to date with any changes in data structure or processing logic.
- Use version control to track changes to metadata over time.
- Ensure that the Azure pipeline is run whenever metadata is updated to keep DBFS in sync.
- Regularly review and validate metadata definitions against actual data to catch any discrepancies.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Update the relevant metadata files
4. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
5. Push to the branch (`git push origin feature/AmazingFeature`)
6. Open a Pull Request

## Security Notes

- The Databricks access token is stored in an Azure DevOps variable group for security. Never commit sensitive information directly to the repository.
- Ensure that access to this repository and the associated Azure DevOps project is restricted to authorized personnel only.

