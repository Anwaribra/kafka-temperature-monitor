CREATE TABLE IF NOT EXISTS temperature_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    temperature FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sensor_id ON temperature_readings(sensor_id);
CREATE INDEX IF NOT EXISTS idx_timestamp ON temperature_readings(timestamp); 