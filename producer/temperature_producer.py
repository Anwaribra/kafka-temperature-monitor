import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'temperature-readings'
NUM_SENSORS = 5
INTERVAL_SECONDS = 2

def create_producer(max_retries=5, retry_interval=5):
    """Create and return a Kafka producer instance with retries."""
    retries = 0
    while retries < max_retries:
        try:
            print(f"Attempting to connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Add these options for better connection handling
                request_timeout_ms=30000,
                connections_max_idle_ms=540000
            )
            
            producer.bootstrap_connected()
            print("Successfully connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            retries += 1
            print(f"No brokers available. Retry {retries}/{max_retries} in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    raise Exception(f"Failed to connect to Kafka after {max_retries} attempts")

def generate_temperature(sensor_id):
    """Generate a random temperature reading for a sensor."""
    
    base_temp = 20 + (hash(sensor_id) % 10)
    
    variation = random.uniform(-2.0, 5.0)
    return round(base_temp + variation, 1)

def simulate_sensors(producer):
    """Simulate temperature readings from multiple sensors."""
    # Create sensor IDs
    sensor_ids = [f"sensor-{uuid.uuid4().hex[:8]}" for _ in range(NUM_SENSORS)]
    
    try:
        while True:
            for sensor_id in sensor_ids:
                
                temperature = generate_temperature(sensor_id)
                
                # Create message
                message = {
                    'sensor_id': sensor_id,
                    'temperature': temperature,
                    'timestamp': datetime.now().isoformat()
                }
                
                # Send to Kafka
                producer.send(KAFKA_TOPIC, message)
                print(f"Sent: {message}")
            
            
            producer.flush()
            
            
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("Stopping temperature simulation...")
    except Exception as e:
        print(f"Error in simulation: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    try:
        print(f"Starting temperature simulation for {NUM_SENSORS} sensors...")
        producer = create_producer()
        simulate_sensors(producer)
    except Exception as e:
        print(f"Fatal error: {e}") 