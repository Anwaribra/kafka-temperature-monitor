# Kafka Temperature Monitoring System

This project simulates a temperature monitoring system using Apache Kafka. It consists of:

1. A producer that simulates multiple temperature sensors
2. A consumer that monitors the temperature readings and stores them in a PostgreSQL database
3. A classification system that categorizes readings as NORMAL, WARNING, or CRITICAL

## Architecture

- **Producer**: Simulates multiple temperature sensors sending readings to Kafka
- **Kafka**: Message broker that handles the temperature reading events
- **Consumer**: Processes the temperature readings, determines their status, and stores them in the database
- **PostgreSQL**: Stores all temperature readings with their status

## Setup and Running

### Prerequisites

- Docker and Docker Compose
- Python 3.8+

### Installation

1. Clone this repository
2. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

### Running the System

1. Start the Kafka and PostgreSQL services:
   ```
   docker-compose up -d
   ```

2. Start the temperature producer (in a separate terminal):
   ```
   python -m producer.temperature_producer
   ```

3. Start the temperature consumer (in a separate terminal):
   ```
   python -m consumer.temperature_consumer
   ```

### Stopping the System

1. Stop the producer and consumer by pressing Ctrl+C in their respective terminals
2. Stop the Docker services:
   ```
   docker-compose down
   ```

## Data Model

Temperature readings are stored with the following information:
- Sensor ID
- Temperature value (in Celsius)
- Timestamp
- Status (NORMAL, WARNING, or CRITICAL)

```
## Status Classification

- **NORMAL**: Temperature ≤ 25°C
- **WARNING**: Temperature between 25°C and 30°C
- **CRITICAL**: Temperature > 30°C
  ```
