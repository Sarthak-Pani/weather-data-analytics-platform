-- =========================================================
-- WEATHER DATA ANALYTICS PLATFORM
-- =========================================================

-- 1️⃣ Create Database
CREATE DATABASE IF NOT EXISTS weather_analytics;
USE weather_analytics;

-- =========================================================
-- 2️⃣ Raw Staging Table (Loaded via Python ETL)
-- =========================================================

DROP TABLE IF EXISTS weather_raw;

CREATE TABLE weather_raw (
    date DATE,
    city VARCHAR(50),
    temperature FLOAT,
    humidity FLOAT,
    rainfall FLOAT
);

-- Data will be inserted here from Python ETL


-- =========================================================
-- 3️⃣ Dimension Tables
-- =========================================================

DROP TABLE IF EXISTS dim_city;

CREATE TABLE dim_city (
    city_id INT PRIMARY KEY AUTO_INCREMENT,
    city_name VARCHAR(50)
);


DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE,
    month INT,
    year INT,
    quarter INT
);


-- =========================================================
-- 4️⃣ Populate Dimension Tables
-- =========================================================

INSERT INTO dim_city (city_name)
SELECT DISTINCT city
FROM weather_raw;

INSERT INTO dim_date (full_date, month, year, quarter)
SELECT DISTINCT
    date,
    MONTH(date),
    YEAR(date),
    QUARTER(date)
FROM weather_raw;


-- =========================================================
-- 5️⃣ Fact Table
-- =========================================================

DROP TABLE IF EXISTS fact_weather;

CREATE TABLE fact_weather (
    fact_id INT PRIMARY KEY AUTO_INCREMENT,
    city_id INT,
    date_id INT,
    temperature INT,
    humidity INT,
    rainfall INT,
    FOREIGN KEY (city_id) REFERENCES dim_city(city_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);


-- =========================================================
-- 6️⃣ Populate Fact Table
-- =========================================================

INSERT INTO fact_weather (city_id, date_id, temperature, humidity, rainfall)
SELECT
    dc.city_id,
    dd.date_id,
    wr.temperature,
    wr.humidity,
    wr.rainfall
FROM weather_raw wr
JOIN dim_city dc ON wr.city = dc.city_name
JOIN dim_date dd ON wr.date = dd.full_date;


-- =========================================================
-- 7️⃣ Indexing for Performance Optimization
-- =========================================================

CREATE INDEX idx_city ON fact_weather(city_id);
CREATE INDEX idx_date ON fact_weather(date_id);


-- =========================================================
-- 8️⃣ Analytical Queries
-- =========================================================

-- Average temperature by city
SELECT 
    dc.city_name,
    AVG(fw.temperature) AS avg_temperature
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
GROUP BY dc.city_name;


-- Monthly average temperature
SELECT 
    dc.city_name,
    dd.month,
    AVG(fw.temperature) AS avg_temperature
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
JOIN dim_date dd ON fw.date_id = dd.date_id
GROUP BY dc.city_name, dd.month
ORDER BY dc.city_name, dd.month;


-- Ranking cities by average temperature (Window Function)
SELECT
    dc.city_name,
    AVG(fw.temperature) AS avg_temperature,
    RANK() OVER (ORDER BY AVG(fw.temperature) DESC) AS temp_rank
FROM fact_weather fw
JOIN dim_city dc ON fw.city_id = dc.city_id
GROUP BY dc.city_name;


-- Highest rainfall per city (Window + Partition)
SELECT *
FROM (
    SELECT
        dc.city_name,
        dd.full_date,
        fw.rainfall,
        RANK() OVER (PARTITION BY dc.city_name ORDER BY fw.rainfall DESC) AS rain_rank
    FROM fact_weather fw
    JOIN dim_city dc ON fw.city_id = dc.city_id
    JOIN dim_date dd ON fw.date_id = dd.date_id
) ranked_data
WHERE rain_rank = 1;


-- =========================================================
-- END OF SCRIPT
-- =========================================================