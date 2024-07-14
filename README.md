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
Parking data is collected from IoT sensors in parking facilities across the city. This data includes:

Station ID
Total parking spots
Available spots
Lattitude
Longitude ...etc

### Events Data
Events data is collected from various sources, including city event calendars and ticketing systems. It includes:

Event name
Event type
Venue
Date and time
Lattitude and Longitude
Expected attendance ...etc

### Weather Data
Weather data is obtained from the Meteomatics API to enrich the events data at the gold layer. This includes:

Temperature
Precipitation
Wind speed
Cloud cover ...etc
