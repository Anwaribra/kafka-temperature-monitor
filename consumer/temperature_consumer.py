import json
import psycopg2
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'temperature-readings'
KAFKA_GROUP_ID = 'temperature-monitor-group'

# Database configuration
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'temperature_db'
DB_USER = 'postgres'
DB_PASSWORD = '2003'

# Temperature thresholds
NORMAL_MAX = 25.0
WARNING_MAX = 30.0  # Above this is critical

def create_consumer(max_retries=5, retry_interval=5):
    """Create and return a Kafka consumer instance with retries."""
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset='earliest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                # Add these options for better connection handling
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
                connections_max_idle_ms=540000
            )
            # Test if the consumer is connected
            consumer.topics()
            print("Successfully connected to Kafka!")
            return consumer
        except NoBrokersAvailable:
            retries += 1
            print(f"No brokers available. Retry {retries}/{max_retries} in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

def get_db_connection(max_retries=5, retry_interval=5):
    """Create and return a database connection with retries."""
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempting to connect to PostgreSQL at {DB_HOST}:{DB_PORT}...")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Successfully connected to PostgreSQL!")
            return conn
        except psycopg2.OperationalError as e:
            retries += 1
            print(f"Database connection failed. Retry {retries}/{max_retries} in {retry_interval} seconds...")
            print(f"Error: {e}")
            time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to PostgreSQL after {max_retries} attempts")

def determine_status(temperature):
    """Determine the status based on temperature."""
    if temperature <= NORMAL_MAX:
        return "NORMAL"
    elif temperature <= WARNING_MAX:
        return "WARNING"
    else:
        return "CRITICAL"

def process_temperature_reading(reading, conn):
    """Process a temperature reading and store it in the database."""
    sensor_id = reading['sensor_id']
    temperature = reading['temperature']
    timestamp = reading['timestamp']
    
    # Determine status
    status = determine_status(temperature)
    
    # Insert into database
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO temperature_readings (sensor_id, temperature, timestamp, status)
            VALUES (%s, %s, %s, %s)
            """,
            (sensor_id, temperature, timestamp, status)
        )
    conn.commit()
    
    # Print status (in real application, you might want to send alerts)
    print(f"Sensor: {sensor_id}, Temp: {temperature}°C, Status: {status}")
    if status == "WARNING":
        print(f"⚠️ WARNING: Temperature for {sensor_id} is high!")
    elif status == "CRITICAL":
        print(f"🔥 CRITICAL: Temperature for {sensor_id} is dangerously high!")

def monitor_temperatures():
    """Monitor temperature readings from Kafka and process them."""
    try:
        consumer = create_consumer()
        conn = get_db_connection()
        
        print("Starting temperature monitoring...")
        for message in consumer:
            reading = message.value
            process_temperature_reading(reading, conn)
    except KeyboardInterrupt:
        print("Stopping temperature monitoring...")
    except Exception as e:
        print(f"Error in monitoring: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    monitor_temperatures() 