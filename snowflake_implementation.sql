-- Create Warehouse
CREATE WAREHOUSE weather_wh
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

-- Create Database
CREATE DATABASE weather_db;

USE DATABASE weather_db;

-- Create Schema
CREATE SCHEMA analytics;

USE SCHEMA analytics;

-- Dimension Tables
CREATE TABLE dim_city (
    city_id INT,
    city_name STRING
);

CREATE TABLE dim_date (
    date_id INT,
    full_date DATE,
    year INT,
    month INT,
    day INT
);

-- Fact Table
CREATE TABLE fact_weather (
    weather_id INT,
    city_id INT,
    date_id INT,
    temperature FLOAT,
    humidity FLOAT,
    rainfall FLOAT
);

-- Clustering for performance optimization
ALTER TABLE fact_weather
CLUSTER BY (city_id, date_id);

-- Time Travel Example Query
SELECT * FROM fact_weather
AT (TIMESTAMP => DATEADD('hour', -1, CURRENT_TIMESTAMP));

-- Semi-Structured Data Example
CREATE TABLE weather_json (
    raw_data VARIANT
);

-- Extract JSON fields
SELECT raw_data:city::STRING,
       raw_data:temperature::FLOAT
FROM weather_json;