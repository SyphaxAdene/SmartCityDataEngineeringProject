## The project is organized into several repositories:

- azure_datafactory/: ADF pipeline definitions and configurations
- dbw_jobs/: Databricks notebooks and job definitions
- kafka_producer/: Kafka producer application for IoT data ingestion
- metadata/: Metadata definitions and management tools
- terraform_azure/: Terraform scripts for infrastructure deployment

## The project enables various analyses to understand urban dynamics:

- Correlation between events and parking utilization
- Impact of weather conditions on parking patterns
- Predictive models for parking demand based on upcoming events and forecasted weather
- Visualization of parking utilization trends over time

## Data Sources
### Parking Data
Parking data is produced by me (mock data) and sent by the kafka producer (kafka repo) to Kafka Topic which is located in the Confluent Cloud. This data includes:

- Station ID
- Total parking spots
- Available spots
- Lattitude
- Longitude ...etc

### Events Data
Events data is produced by me (mock data) in my local computer and send them to Azure SFTP using the WINSCP tool, Then ingested by ADF from SFTP to Bronze Layer. It includes:

- Event name
- Event type
- Venue
- Date and time
- Lattitude and Longitude
- Expected attendance ...etc

### Weather Data
Weather data is obtained from the Meteomatics API to enrich the events data at the gold layer. This includes:

- Temperature
- Precipitation
- Wind speed
- Cloud cover ...etc
