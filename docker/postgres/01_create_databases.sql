-- Create airflow database separately
CREATE DATABASE airflow_db;

-- Grant all permissions to our user
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO supplychain;
GRANT ALL PRIVILEGES ON DATABASE supplychain_db TO supplychain;