# Kafka Project

## Overview

This project provides a Kafka producer and consumer implementation for handling IoT data. The producer reads JSON files containing parking station data and publishes them to a Kafka topic. The consumer subscribes to the Kafka topic and processes the received messages.

# Project Structure

- kafka_project/
  - data/
    - [subfolder_name]/
      - *.json
  - kafka_consumer.py
  - kafka_producer.py
  - config.json
  - config.template.json
  - requirements.txt
  - .gitignore


## Configuration

### Configuration Files

- `config.template.json`: Template for the Kafka configuration.
- `config.json`: Actual configuration used by the producer and consumer. This file should be filled with your Kafka cluster details and credentials.

### Environment Variables

The following environment variables can override the values in `config.json`:

- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_SASL_MECHANISMS`
- `KAFKA_SECURITY_PROTOCOL`
- `KAFKA_SASL_USERNAME`
- `KAFKA_SASL_PASSWORD`

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/kafka_project.git
    cd kafka_project
    ```

2. Create a virtual environment and activate it:

    ```bash
    python3 -m venv kafka_env
    source kafka_env/bin/activate  # On Windows use `kafka_env\Scripts\activate`
    ```

3. Install the required packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Kafka Producer

The producer reads JSON files from a specified subfolder under the `data` directory and publishes them to the Kafka topic `iot_data_topic`.

1. Prepare your data files in a subfolder inside the `data` directory.
2. Run the producer:

    ```bash
    python kafka_producer.py <subfolder_name>
    ```

    Replace `<subfolder_name>` with the name of your subfolder containing JSON files.

### Kafka Consumer

The consumer subscribes to the `iot_data_topic` topic and processes incoming messages.

1. Run the consumer:

    ```bash
    python kafka_consumer.py
    ```

## Example JSON File

Here is an example of a JSON file that can be used with this project:

```json
{
    "stationId": "STATION311",
    "name": "Convention Center West Parking",
    "location": {
        "address": "9860 Universal Blvd, Orlando, FL 32819",
        "latitude": 28.4257,
        "longitude": -81.4709
    },
    "capacity": {
        "totalSpots": 2500,
        "availableSpots": 750
    },
    "rates": {
        "hourlyRate": 4.00,
        "dailyRate": 30.00,
        "monthlyRate": 500.00
    },
    "operatingHours": {
        "monday": {
            "open": "05:00",
            "close": "23:59"
        },
        "tuesday": {
            "open": "05:00",
            "close": "23:59"
        },
        "wednesday": {
            "open": "05:00",
            "close": "23:59"
        },
        "thursday": {
            "open": "05:00",
            "close": "23:59"
        },
        "friday": {
            "open": "05:00",
            "close": "23:59"
        },
        "saturday": {
            "open": "05:00",
            "close": "23:59"
        },
        "sunday": {
            "open": "05:00",
            "close": "23:59"
        }
    },
    "amenities": {
        "hasEVCharging": true,
        "hasHandicapAccess": true,
        "hasSecurityCameras": true,
        "acceptsCreditCard": true
    },
    "contactInfo": {
        "phone": "+1-407-555-4321",
        "email": "parking@occc.com"
    },
    "lastUpdated": "2024-07-10T09:30:00Z"
}
```
.gitignore

The .gitignore file is set up to ignore the following files and directories:

/kafka_env

config.json