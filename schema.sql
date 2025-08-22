CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(100),
    temperature_c FLOAT,
    humidity INT,
    observed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS country_info (
    id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    capital VARCHAR(100),
    population BIGINT,
    currency VARCHAR(50)
);

