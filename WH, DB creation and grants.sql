-- Creating a warehouse for the project
CREATE WAREHOUSE ecom_analytics_wh;
 
-- Create a database for the e-commerce analytics project
CREATE OR REPLACE DATABASE ecom_analytics_db;

-- Create a raw schema
CREATE OR REPLACE SCHEMA ecom_analytics_db.raw_data;

-- Create an internal stage where I will put the csv files
CREATE OR REPLACE STAGE raw_data.csv_stage
  FILE_FORMAT = raw_data.csv_ff;

-- Granting Access
-- 2. Let My role use the warehouse where you’ll load data
GRANT USAGE ON WAREHOUSE ecom_analytics_wh
  TO ROLE BEETLE_ROLE;

-- 3. Let my Role in Snowflake see and use the project database & raw_data schema
GRANT USAGE ON DATABASE ecom_analytics_db
  TO ROLE BEETLE_ROLE;
GRANT USAGE ON SCHEMA ecom_analytics_db.raw_data
  TO ROLE BEETLE_ROLE;

-- 4. Let it create/load tables in raw_data
GRANT CREATE TABLE ON SCHEMA ecom_analytics_db.raw_data
  TO ROLE BEETLE_ROLE;

-- 5. Grant the two stage privileges needed for PUT / GET
GRANT READ, WRITE
  ON STAGE ecom_analytics_db.raw_data.csv_stage
  TO ROLE BEETLE_ROLE;


-- Revoke all privileges from TRAINING_ROLE on the database
REVOKE ALL PRIVILEGES ON DATABASE ecom_analytics_db FROM ROLE TRAINING_ROLE;

-- Revoke on each schema (precautionary cleanup)
REVOKE ALL PRIVILEGES ON SCHEMA ecom_analytics_db.raw_data FROM ROLE TRAINING_ROLE;
