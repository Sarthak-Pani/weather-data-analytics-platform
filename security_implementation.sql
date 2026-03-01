-- Create Roles
CREATE ROLE analyst_role;
CREATE ROLE admin_role;

-- Grant usage
GRANT USAGE ON DATABASE weather_db TO ROLE analyst_role;
GRANT USAGE ON SCHEMA analytics TO ROLE analyst_role;

-- Grant select only on fact table
GRANT SELECT ON TABLE fact_weather TO ROLE analyst_role;

-- Admin full access
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO ROLE admin_role;

-- Data Masking Policy
CREATE MASKING POLICY mask_temperature
AS (val FLOAT) 
RETURNS FLOAT ->
CASE 
    WHEN CURRENT_ROLE() IN ('admin_role') THEN val
    ELSE NULL
END;

ALTER TABLE fact_weather 
MODIFY COLUMN temperature 
SET MASKING POLICY mask_temperature;